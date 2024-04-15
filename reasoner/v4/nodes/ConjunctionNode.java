package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.ConjunctionStreamPlan.CompoundStreamPlan;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class ConjunctionNode extends ActorNode<ConjunctionNode> {

    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionNode.class);
    private final ResolvableConjunction conjunction;
    private final ConceptMap bounds;
    private final CompoundStreamPlan compoundStreamPlan;
    private Port leftChildPort;
    private Map<Port, ConceptMap> rightPortExtensions;

    public ConjunctionNode(ResolvableConjunction conjunction, ConceptMap bounds, CompoundStreamPlan compoundStreamPlan, NodeRegistry nodeRegistry, Driver<ConjunctionNode> driver) {
        super(nodeRegistry, driver, () -> "ConjunctionNode[" + conjunction + ", " + bounds + "]");
        this.conjunction = conjunction;
        this.bounds = bounds;
        this.compoundStreamPlan = compoundStreamPlan;
        nodeRegistry.perfCounterFields().subConjunctionNodes.add(1);
    }

    @Override
    protected void initialise() {
        super.initialise();
        assert this.leftChildPort == null;
        this.leftChildPort = createPort(nodeRegistry.getRegistry(leftPlan()).getNode(bounds.filter(leftPlan().identifiers())));
        this.rightPortExtensions = new HashMap<>();
    }

    @Override
    protected void propagatePull(ActorNode.Port reader, int index) {
        answerTable.registerSubscriber(reader, index);
        allPorts().forEachRemaining(port -> {
            if (port.isReady()) port.readNext();
        });
    }

    @Override
    protected void handleAnswer(Port onPort, Message.Answer received) {
        if (onPort == leftChildPort) receiveLeft(onPort, received.asAnswer().answer());
        else receiveRight(onPort, received.asAnswer().answer());
    }

    @Override
    protected void handleDone(Port onPort) {
        if (allPortsDone()) {
            FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        } else if (!anyPortsActive()) {
            throw TypeDBException.of(UNIMPLEMENTED);
        }
    }

    private void receiveLeft(ActorNode.Port onPort, ConceptMap answer) {
        assert onPort == leftChildPort;
        Set<Identifier.Variable.Retrievable> extensionVars = Collections.intersection(answer.concepts().keySet(), compoundStreamPlan.outputs());
        ConceptMap rightChildBounds = merge(answer, this.bounds).filter(rightPlan().identifiers());
        ActorNode<?> newRightChildNode = nodeRegistry.getRegistry(rightPlan()).getNode(rightChildBounds);
        Port newRightChildPort = createPort(newRightChildNode);
        newRightChildPort.readNext(); // KGFLAG: Strategy
        rightPortExtensions.put(newRightChildPort, answer.filter(extensionVars));

        onPort.readNext(); // readL
    }

    public void receiveRight(ActorNode.Port onPort, ConceptMap answer) {
        FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
        ConceptMap extendedAnswer = merge(merge(rightPortExtensions.get(onPort), answer), bounds).filter(compoundStreamPlan.outputs());
        Message toSend = answerTable.recordAnswer(extendedAnswer);
        subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        onPort.readNext(); // KGFLAG: Strategy
    }

    private ConceptMap merge(ConceptMap into, ConceptMap from) {
        Map<Identifier.Variable.Retrievable, Concept> compounded = new HashMap<>(into.concepts());
        compounded.putAll(from.concepts());
        return new ConceptMap(compounded, into.explainables().merge(from.explainables()));
    }

    private ConjunctionController.ConjunctionStreamPlan leftPlan() {
        return compoundStreamPlan.childAt(0);
    }

    private ConjunctionController.ConjunctionStreamPlan rightPlan() {
        return compoundStreamPlan.childAt(1);
    }
}
