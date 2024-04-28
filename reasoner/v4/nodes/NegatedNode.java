package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.reasoner.v4.Message;

public class NegatedNode extends ResolvableNode<Negated, NegatedNode> {


    public NegatedNode(Negated resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<com.vaticle.typedb.core.reasoner.v4.nodes.NegatedNode> driver) {
        super(resolvable, bounds, nodeRegistry, driver);
    }

    @Override
    public void initialise() {
        super.initialise();
        resolvable.disjunction().conjunctions().forEach(conjunction -> {
            createPort(nodeRegistry.getRegistry(nodeRegistry.conjunctionStreamPlan(conjunction, bounds)).getNode(bounds));
        });
    }

    @Override
    public void propagatePull(Port reader, int index) {
        assert index == 0 && answerTable.answerAt(index).isEmpty();
        answerTable.registerSubscriber(reader, index);
        ports.forEach(port -> {
            assert port.isReady();
            port.readNext();
        });
    }

    @Override
    protected void doHandleAnswer(Port onPort, Message.Answer asAnswer) {
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
            // And we're done. No more pulling.
        }
    }

    @Override
    protected void handleDone(Port onPort) {
        if (!answerTable.isComplete()) super.handleDone(onPort);
    }

    @Override
    protected void onTermination() {
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordAnswer(bounds);
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
            answerTable.recordDone();
        }
    }
}
