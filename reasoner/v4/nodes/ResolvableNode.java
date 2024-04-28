package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.reasoner.v4.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class ResolvableNode<RESOLVABLE extends Resolvable<?>, NODE extends ResolvableNode<RESOLVABLE, NODE>>
        extends ActorNode<NODE> {

    private static final Logger LOG = LoggerFactory.getLogger(ResolvableNode.class);

    protected final RESOLVABLE resolvable;
    protected final ConceptMap bounds;

    public ResolvableNode(RESOLVABLE resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<NODE> driver) {
        super(nodeRegistry, driver, () -> String.format("ResolvableNode[%s, %s]", resolvable, bounds));
        this.resolvable = resolvable;
        this.bounds = bounds;
        nodeRegistry.perfCounterFields().resolvableNodes.add(1);
    }

    @Override
    public String toString() {
        return String.format("%s[%s::%s]", this.getClass().getSimpleName(), this.resolvable.pattern(), this.bounds);
    }

    public abstract static class RetrievalNode<RES extends Resolvable<Conjunction>, RESNODE extends ResolvableNode<RES, RESNODE>>
            extends ResolvableNode<RES, RESNODE> {

        private final FunctionalIterator<ConceptMap> traversal;

        public RetrievalNode(RES resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<RESNODE> driver) {
            super(resolvable, bounds, nodeRegistry, driver);
            this.traversal = Traversal.traversalIterator(nodeRegistry, resolvable.pattern(), bounds);
        }

        @Override
        public void propagatePull(ActorNode.Port reader, int index) {
            assert answerTable.answerAt(index).isEmpty();
            send(reader.owner(), reader, pullTraversalSynchronous());
        }

        private Message pullTraversalSynchronous() {
            return traversal.hasNext() ?
                    answerTable.recordAnswer(traversal.next()) :
                    answerTable.recordDone();
        }

        @Override
        public void receive(ActorNode.Port onPort, Message message) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        @Override
        protected void doHandleAnswer(Port onPort, Message.Answer answer) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        @Override
        protected void handleDone(Port onPort) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

    }

    public static class RetrievableNode extends RetrievalNode<Retrievable, RetrievableNode> {
        public RetrievableNode(Retrievable retrievable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<RetrievableNode> driver) {
            super(retrievable, bounds, nodeRegistry, driver);
        }
    }
}
