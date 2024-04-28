package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.v4.Message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConcludableNode extends ResolvableNode<Concludable, ConcludableNode> {
    // TODO: See if I can get away without storing answers
    private final Set<ConceptMap> seenAnswers;
    private Map<Port, Pair<Unifier, Unifier.Requirements.Instance>> conclusioNodePorts; // TODO: Improve
    private Port lookupPort;

    public ConcludableNode(Concludable concludable, ConceptMap bounds,
                           NodeRegistry nodeRegistry, Driver<com.vaticle.typedb.core.reasoner.v4.nodes.ConcludableNode> driver) {
        super(concludable, bounds, nodeRegistry, driver);
        this.seenAnswers = new HashSet<>();
        this.conclusioNodePorts = null;
    }

    @Override
    protected void initialise() {
        super.initialise();
        assert conclusioNodePorts == null;
        this.conclusioNodePorts = new HashMap<>();
        Driver<ConcludableLookupNode> lookupNode = nodeRegistry.createLocalNode(driver -> new ConcludableLookupNode(resolvable, bounds, nodeRegistry, driver));
        this.lookupPort = createPort(lookupNode.actor());

        nodeRegistry.logicManager().applicableRules(resolvable).forEach((rule, unifiers) -> {
            unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                ActorNode<?> conclusionNode = nodeRegistry.conclusionSubRegistry(rule.conclusion()).getNode(boundsAndRequirements.first());
                conclusioNodePorts.put(createPort(conclusionNode), new Pair<>(unifier, boundsAndRequirements.second()));
            }));
        });
    }


    @Override
    protected void propagatePull(Port reader, int index) {
        answerTable.registerSubscriber(reader, index);

        // KGFLAG: Strategy
        if (lookupPort.isReady()) lookupPort.readNext();
        conclusioNodePorts.keySet().forEach(port -> {
            if (port.isReady()) port.readNext();
        });
    }

    protected void doHandleAnswer(Port onPort, Message.Answer received) {
        assert onPort == lookupPort;
        recordAndForwardAnswers(Iterators.single(received.answer()));
        onPort.readNext();
    }

    protected void handleConclusion(Port onPort, Message.Conclusion received) {
        Pair<Unifier, Unifier.Requirements.Instance> unifierAndRequirements = conclusioNodePorts.get(onPort);
        recordAndForwardAnswers(unifierAndRequirements.first()
                .unUnify(received.conclusionAnswer(), unifierAndRequirements.second()));
        onPort.readNext();
    }

    private void recordAndForwardAnswers(FunctionalIterator<ConceptMap> answers) {
        answers.forEachRemaining(conceptMap -> {
            if (seenAnswers.contains(conceptMap)) return;
            seenAnswers.add(conceptMap);
            // We can do this multiple times, since subscribers will be empty
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordAnswer(conceptMap);
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        });
    }

    public static class ConcludableLookupNode extends RetrievalNode<Concludable, ConcludableLookupNode> {
        public ConcludableLookupNode(Concludable concludable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConcludableLookupNode> driver) {
            super(concludable, bounds, nodeRegistry, driver);
        }
    }
}
