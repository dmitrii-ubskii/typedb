package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.v4.nodes.AnswerTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends Actor<NODE> {

    private static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    protected final NodeRegistry nodeRegistry;
    protected List<ActorNode.Port> ports;
    private final Set<Port> activePorts;
    private final Set<Port> pendingPorts;
    protected final Integer nodeId;
    protected final AnswerTable answerTable;


    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
        nodeId = nodeRegistry.nextNodeAge();
        ports = new ArrayList<>();
        activePorts = new HashSet<>();
        pendingPorts = new HashSet<>();
        answerTable = new AnswerTable();
    }

    protected void initialise() {

    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    public void readAnswerAt(ActorNode.Port reader, int index) {
        Optional<Message> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            send(reader.owner, reader, peekAnswer.get());
            return;
        } else if (reader.owner.nodeId >= nodeId) { //  strictly < would let you loop.
            send(reader.owner, reader, new Message.HitElder(nodeId, answerTable.size()));
        } else {
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void propagatePull(Port reader, int index);

    public void receive(Port onPort, Message received) {
        switch (received.type()) {
            case ANSWER: {
                handleAnswer(onPort, received.asAnswer());
                break;
            }
            case CONCLUSION: {
                handleConclusion(onPort, received.asConclusion());
                break;
            }
            case DONE: {
                handleDone(onPort);
                break;
            }
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    protected abstract void handleAnswer(Port onPort, Message.Answer answer);

    protected void handleConclusion(Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract void handleDone(Port onPort);

    protected Port createPort(ActorNode<?> remote) {
        Port port = new Port(this, remote);
        ports.add(port);
        activePorts.add(port);

        return port;
    }

    protected boolean allPortsDone() {
        return activePorts.isEmpty() && pendingPorts.isEmpty();
    }

    protected boolean anyPortsActive() {
        return !activePorts.isEmpty();
    }

    protected FunctionalIterator<ActorNode.Port> allPorts() {
        return Iterators.iterate(ports);
    }

    // TODO: See if i can safely get recipient from port
    public void send(ActorNode<?> recipient, ActorNode.Port recipientPort, Message message) {
        assert recipientPort.remote == this;
        recipient.driver().execute(actor -> actor.receiveOnPort(recipientPort, message));
    }

    protected void receiveOnPort(Port port, Message message) {
        LOG.debug(port.owner() + " received " + message + " from " + port.remote());
        port.recordReceive(message); // Is it strange that I call this implicitly?
        receive(port, message);
    }

    private void recordDone(ActorNode.Port port) {
        if (activePorts.contains(port)) activePorts.remove(port);
        else if (pendingPorts.contains(port)) pendingPorts.remove(port);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void exception(Throwable e) {
        nodeRegistry.terminate(e);
    }

    @Override
    public void terminate(Throwable e) {
        super.terminate(e);
    }

    public static class Port {

        public enum State {READY, PULLING, DONE}
        private final ActorNode<?> owner;
        private final ActorNode<?> remote;
        private State state;
        private int lastRequestedIndex;
        private boolean isPending;

        private Port(ActorNode<?> owner, ActorNode<?> remote) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.isPending = false;
        }

        private void recordReceive(Message msg) {
            assert state == State.PULLING;

            assert lastRequestedIndex == msg.index();
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
                owner.recordDone(this);
            } else {
                state = State.READY;
            }
            assert state != State.PULLING;
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
