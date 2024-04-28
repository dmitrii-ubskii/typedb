package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private final List<ActorNode.Port> downstreamPorts;
    private Message.HitInversion forwardedInversion;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        forwardedInversion = null;
        downstreamPorts = new ArrayList<>();
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    public void readAnswerAt(ActorNode.Port reader, int index) {
        Optional<Message> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            send(reader.owner, reader, peekAnswer.get());
        } else if (reader.owner.nodeId >= this.nodeId) {
            send(reader.owner, reader, new Message.HitInversion(this.nodeId, true, -1));
        } else {
            // TODO: Is this a problem? If it s already pulling, we have no clean way of handling it
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void handleAnswer(Port onPort, Message.Answer answer);

    protected void handleConclusion(Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void handleHitInversion(Port onPort, Message.HitInversion hitInversion) {
        checkInversionStatusChange();
    }

    @Override
    protected void handleDone(Port onPort) {
        if (checkTermination()) {
            onTermination();
        } else checkInversionStatusChange();
    }

    protected void checkInversionStatusChange() {
        Optional<Message.HitInversion> oldestInversion = findOldestInversionStatus();
        if (oldestInversion.isEmpty()) return;
        if (forwardedInversion == null || !forwardedInversion.equals(oldestInversion.get())) {
            forwardedInversion = oldestInversion.get();
            // TODO: Check if it's termination time.
            if (forwardedInversion.nodeId == this.nodeId) {
                if (forwardedInversion.throughAllPaths && forwardedInversion.index() == answerTable.size()) {
                    throw TypeDBException.of(UNIMPLEMENTED); // TODO: Also need to declare DONE
                } else {
                    downstreamPorts.forEach(port -> {
                        send(port.owner, port, new Message.HitInversion(this.nodeId, true, answerTable.size()));
                    });
                    LOG.debug("Received this.nodeId={} on all ports, but tableSie {} < {}",
                            this.nodeId, forwardedInversion.index(), answerTable.size());
                }
            } else {
                downstreamPorts.forEach(port -> send(port.owner, port, forwardedInversion));
            }
        }
    }

    protected boolean checkTermination() {
        return allPortsDone();
    }

    protected void onTermination() {
        assert allPortsDone();
        FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
        Message toSend = answerTable.recordDone();
        subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
    }

    private Optional<Message.HitInversion> findOldestInversionStatus() {
        Comparator<Message.HitInversion> comparator = (a, b) -> {
            if (a == null) { return b == null ? 0 : 1; }
            else if (b == null) return -1;
            int res;
            if (0 == (res = Integer.compare(a.nodeId, b.nodeId))) {
                if (0 == (res = Integer.compare(b.index(), a.index())))  { ; // Note: a and b are swapped - Bigger index, better
                    res = Boolean.compare(b.throughAllPaths, a.throughAllPaths); // These are also swapped because true is better
                }
            }
            return res;
        };
        Message.HitInversion bestInversion = activePorts.stream()
                .map(port -> port.receivedInversion).filter(Objects::nonNull)
                .min(comparator).orElse(null);
        if (bestInversion == null) return Optional.empty();
        else {
            boolean throughAllPaths = bestInversion.throughAllPaths && activePorts.stream().map(p->p.receivedInversion)
                    .allMatch(otherInversion -> 0 == comparator.compare(bestInversion, otherInversion));
            return Optional.of(new Message.HitInversion(bestInversion.nodeId, throughAllPaths, bestInversion.index()));
        }
    }


    protected Port createPort(ActorNode<?> remote) {
        Port port = new Port(this, remote);
        remote.notifyPortCreated(port);
        ports.add(port);
        activePorts.add(port);
        return port;
    }

    private void notifyPortCreated(Port downstream) {
        this.downstreamPorts.add(downstream);
    }

    public static class Port {

        public enum State {READY, PULLING, DONE}
        private final ActorNode<?> owner;
        private final ActorNode<?> remote;
        private State state;
        private int lastRequestedIndex;
        private Message.HitInversion receivedInversion;

        protected Port(ActorNode<?> owner, ActorNode<?> remote) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.receivedInversion = null;
        }

        protected void recordReceive(Message msg) {
            // assert state == State.PULLING; // Relaxed for HitInversion
            assert msg.type() == Message.MessageType.HIT_INVERSION || lastRequestedIndex == msg.index();
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
            } else if (msg.type() == Message.MessageType.HIT_INVERSION) {
                this.receivedInversion = msg.asHitInversion();
                // state = State.READY;
            } else {
                state = State.READY;
            }
            // assert state != State.PULLING;
        }


        public void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            lastRequestedIndex += 1;
            int readIndex = lastRequestedIndex;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, readIndex));
        }

        public ActorNode<?> owner() {
            return owner;
        }

        public ActorNode<?>  remote() {
            return remote;
        }

        public State state() {
            return state;
        }

        public int lastRequestedIndex() {
            return lastRequestedIndex;
        }

        public boolean isReady() { return state == State.READY; }
    }
}
