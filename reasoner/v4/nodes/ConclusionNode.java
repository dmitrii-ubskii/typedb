package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.v4.Message;

import java.util.Optional;

public class ConclusionNode extends ActorNode<ConclusionNode> {
    private final Rule.Conclusion conclusion;
    private final ConceptMap bounds;
    private int pendingMaterialisations;

    public ConclusionNode(Rule.Conclusion conclusion, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConclusionNode> driver) {
        super(nodeRegistry, driver, () -> String.format("Conclusion[%s, %s, %s]", conclusion.rule(), conclusion, bounds));
        this.conclusion = conclusion;
        this.bounds = bounds;
        this.pendingMaterialisations = 0;
    }

    @Override
    public void initialise() {
        conclusion.rule().condition().disjunction().conjunctions().forEach(conjunction -> {
            ConjunctionController.ConjunctionStreamPlan csPlan = nodeRegistry.conjunctionStreamPlan(conjunction, bounds);
            createPort(nodeRegistry.getRegistry(csPlan).getNode(bounds));
        });
    }

    protected void propagatePull(ActorNode.Port reader, int index) {
        answerTable.registerSubscriber(reader, index);

        // KGFLAG: Strategy
        ports.forEach(port -> {
            if (port.isReady()) port.readNext();
        });
    }

    @Override
    protected void doHandleAnswer(Port onPort, Message.Answer answer) {
        requestMaterialisation(onPort, answer);
        // Do NOT readNext.
    }

    private void requestMaterialisation(Port onPort, Message.Answer whenConcepts) {
        pendingMaterialisations += 1;
        nodeRegistry.materialiserNode()
                .execute(materialiserNode -> materialiserNode.materialise(this, onPort, whenConcepts, conclusion));
    }

    public void receiveMaterialisation(Port port, Optional<Message.Conclusion> thenConcepts) {
        pendingMaterialisations -= 1;
        if (thenConcepts.isPresent()) {
            FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordConclusion(thenConcepts.get().conclusionAnswer());
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        }
        if (port.isReady()) port.readNext();
        if (checkTermination()) {
            onTermination();
        } else checkInversionStatusChange();
    }

    @Override
    protected void checkInversionStatusChange() {
        if (pendingMaterialisations > 0) return;
        else super.checkInversionStatusChange();
    }

    @Override
    protected int DEBUG__pendingMaterialisations() {
        return pendingMaterialisations;
    }

    @Override
    protected boolean checkTermination() {
        if (pendingMaterialisations > 0) {
            return false;
        } else {
            return super.checkTermination();
        }
    }
}
