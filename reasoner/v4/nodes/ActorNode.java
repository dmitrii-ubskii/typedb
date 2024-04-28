package com.vaticle.typedb.core.reasoner.v4.nodes;

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
    private Message.InversionStatus forwardedInversion;
    private Message.TerminateSCC forwardedTermination;
    private static final Comparator<Message.InversionStatus> hitInversionComparator = (a, b) -> {
        if (a == null) { return b == null ? 0 : 1; }
        else if (b == null) return -1;
        int res;
        if (0 == (res = Integer.compare(a.nodeId, b.nodeId))) {
            if (0 == (res = Integer.compare(b.nodeTableSize, a.nodeTableSize)))  { ; // Note: a and b are swapped - Bigger index, better
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
        } else {
            if (reader.owner.nodeId >= this.nodeId) {
                Message.InversionStatus inversion = forwardedInversion != null ? forwardedInversion : new Message.InversionStatus(this.nodeId, -1, true);
                send(reader.owner, reader, new Message.HitInversion(inversion, answerTable.size()));
            }
            propagatePull(reader, index);
        }
    }

    protected void handleAnswer(ActorNode.Port onPort, Message.Answer answer) {
        doHandleAnswer(onPort, answer);
        checkInversionStatusChange();
    }

    protected abstract void doHandleAnswer(Port onPort, Message.Answer answer);

    @Override
    protected void handleHitInversion(Port onPort, Message.HitInversion hitInversion) {
        if (forwardedTermination != null) return;
        checkInversionStatusChange();
    }

    @Override
    protected void handleTerminateSCC(ActorNode.Port onPort, Message.TerminateSCC terminateSCC) {
        // This is basically copying what Done does, but sends the terminateSCC message instead.
        if (0 == hitInversionComparator.compare(terminateSCC.expectedInversion(), forwardedInversion)) {
            if (forwardedTermination == null) {
                answerTable.clearAndReturnSubscribers(answerTable.size());
                answerTable.recordDone();
                forwardedTermination = new Message.TerminateSCC(terminateSCC.expectedInversion(), answerTable.size());
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
        Message.InversionStatus oldestInversion = findOldestInversionStatus().orElse(null);
        if (oldestInversion == null) return;
        // TODO: Check if it's termination time.
        if (oldestInversion.nodeId == this.nodeId) {
            if (oldestInversion.throughAllPaths && oldestInversion.nodeTableSize == answerTable.size()) {
                assert 0 == hitInversionComparator.compare(oldestInversion, forwardedInversion);
                // TODO: May need to declare DONE in both directions, else self-sustaining cycles can exist
                Message.TerminateSCC terminateMsg = new Message.TerminateSCC(forwardedInversion, answerTable.size() + 1);
                // Fake receiving from the active ports
                activePorts.forEach(port -> handleTerminateSCC(port, terminateMsg));
            } else if (forwardedInversion == null || !oldestInversion.equals(forwardedInversion)) {
                System.err.printf("Received this.nodeId=%d on all ports, but tableSize %d < %d or throughAllPaths: %s\n",
                        this.nodeId, oldestInversion.nodeTableSize, answerTable.size(), oldestInversion.throughAllPaths);
                forwardedInversion = new Message.InversionStatus(this.nodeId, answerTable.size(), true);
                downstreamPorts.forEach(port -> {
                    send(port.owner, port, new Message.HitInversion(forwardedInversion, answerTable.size()));
                });
            }
        } else {
            forwardedInversion = oldestInversion;
            downstreamPorts.forEach(port -> send(port.owner, port, new Message.HitInversion(forwardedInversion, answerTable.size())));
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

    private Optional<Message.InversionStatus> findOldestInversionStatus() {
        Message.InversionStatus bestInversion = activePorts.stream()
                .map(port -> port.receivedInversion).filter(Objects::nonNull).map(Message.HitInversion::inversionStatus)
                .min(hitInversionComparator).orElse(null);
        if (bestInversion == null) return Optional.empty();
        else {
            assert DEBUG__pendingMaterialisations() == 0;
            // TODO: true also requires the status is what's expected on the port.
            boolean throughAllPaths = bestInversion.throughAllPaths &&
                    activePorts.stream().allMatch(p -> {
                        return p.receivedInversion != null && p.receivedInversion.index() <= p.lastRequestedIndex() &&
                            0 == hitInversionComparator.compare(bestInversion, p.receivedInversion.inversionStatus());
                    });
            return Optional.of(new Message.InversionStatus(bestInversion.nodeId, bestInversion.nodeTableSize, throughAllPaths));
        }
    }

    protected int DEBUG__pendingMaterialisations() {
        return 0;
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
                    assert state == State.PULLING && lastRequestedIndex == msg.index();
                    state = State.READY;
                    break;
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
