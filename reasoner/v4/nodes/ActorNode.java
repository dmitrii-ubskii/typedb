package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
                    throw TypeDBException.of(UNIMPLEMENTED);
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
        int oldestNodeId = Integer.MAX_VALUE;
        int oldestCount = 0;
        int highestIndex = 0;
        int lowestIndex = 0;
        boolean throughAllPaths = true;
        for (Port port: activePorts) {
            if(port.receivedInversion == null) continue;
            if (port.receivedInversion.nodeId < oldestNodeId) {
                oldestNodeId = port.receivedInversion.nodeId;
                oldestCount = 0;
                highestIndex = port.receivedInversion.index();
                lowestIndex = port.receivedInversion.index();
            }
            oldestCount += 1;
            throughAllPaths = throughAllPaths && port.receivedInversion.throughAllPaths;
        }

        if (oldestNodeId == Integer.MAX_VALUE) return Optional.empty();
        else {
            return Optional.of(
                new Message.HitInversion(
                        oldestNodeId,
                        activePorts.size() == oldestCount && throughAllPaths && highestIndex == lowestIndex,
                        highestIndex
            ));
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
