package com.vaticle.typedb.core.reasoner.v4.nodes;

We make the mistake of forwarding inversion status messages before we have even read the available messages.

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private final List<ActorNode.Port> downstreamPorts;
    private Message.HitInversion forwardedInversion;
    private Message.TerminateSCC forwardedTermination;
    private static final Comparator<Message.HitInversion> hitInversionComparator = (a, b) -> {
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

    @Override
    protected void handleHitInversion(Port onPort, Message.HitInversion hitInversion) {
        checkInversionStatusChange();
    }

    @Override
    protected void handleTerminateSCC(ActorNode.Port onPort, Message.TerminateSCC terminateSCC) {
        // This is basically copying what Done does, but sends the terminateSCC message instead.
        if (0 == hitInversionComparator.compare(terminateSCC.expectedInversion(), forwardedInversion)) {
            if (forwardedTermination == null) {
                forwardedTermination = new Message.TerminateSCC(terminateSCC.expectedInversion(), answerTable.size());
                answerTable.clearAndReturnSubscribers(answerTable.size());
                answerTable.recordDone();
                downstreamPorts.forEach(port -> send(port.owner, port, forwardedTermination));
            }
            assert 0 == hitInversionComparator.compare(forwardedTermination.expectedInversion(),  terminateSCC.expectedInversion());
        } else {
            // Treat this as a regular DONE message
            recordDone(onPort);
            handleDone(onPort);
        }
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
                    // TODO: May need to declare DONE in both directions, else self-sustaining cycles can exist
                    Message.TerminateSCC terminateMsg = new Message.TerminateSCC(forwardedInversion, answerTable.size());
                    // Fake receiving from the actige ports
                    activePorts.forEach(port -> handleTerminateSCC(port, terminateMsg));
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
        Message.HitInversion bestInversion = activePorts.stream()
                .map(port -> port.receivedInversion).filter(Objects::nonNull)
                .min(hitInversionComparator).orElse(null);
        if (bestInversion == null) return Optional.empty();
        else {
            boolean throughAllPaths = bestInversion.throughAllPaths && activePorts.stream().map(p->p.receivedInversion)
                    .allMatch(otherInversion -> 0 == hitInversionComparator.compare(bestInversion, otherInversion));
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
            switch (msg.type()) {
                case HIT_INVERSION:
                    this.receivedInversion = msg.asHitInversion();
                    break;
                case ANSWER:
                case CONCLUSION:
                    assert state == State.PULLING && lastRequestedIndex == msg.index();
                    state = State.READY;
                    break;
                case DONE:
                case TERMINATE_SCC:
                    state = State.DONE;
                    break;
            }
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
