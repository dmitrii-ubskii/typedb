/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.ExplainablesManager;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.EXCEPTION;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.FINISHED;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.INIT;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.INITIALISING;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.PULLING;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.READY;


@ThreadSafe
public abstract class ReasonerProducerV4<ROOTNODE extends ActorNode<ROOTNODE>, ANSWER> implements Producer<ANSWER>, ReasonerConsumerV4 {

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducerV4.class);

    protected final NodeRegistry nodeRegistry;
    private final ExplainablesManager explainablesManager;
    final AtomicInteger requiredAnswers;
    final Options.Query options;
    private Throwable exception;
    Queue<ANSWER> queue;
    State state;
    protected ROOTNODE rootNode; // TODO: Make final, init in constructor, change return type of initialiseRoot

    enum State {
        INIT,
        INITIALISING,
        READY,
        PULLING,
        FINISHED,
        EXCEPTION
    }

    // TODO: this class should not be a Producer, it implements a different async processing mechanism
    private ReasonerProducerV4(Options.Query options, NodeRegistry nodeRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.nodeRegistry = nodeRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.requiredAnswers = new AtomicInteger();
        this.state = INIT;
    }

    NodeRegistry nodeRegistry() {
        return nodeRegistry;
    }

    @Override
    public synchronized void produce(Queue<ANSWER> queue, int requestedAnswers, Executor executor) {
        assert this.queue == null || this.queue == queue;
        assert requestedAnswers > 0;
        if (state == EXCEPTION) queue.done(exception);
        else if (state == FINISHED) queue.done();
        else {
            this.queue = queue;
            requiredAnswers.addAndGet(requestedAnswers);
            if (state == INIT) initialise();
            else if (state == READY) pull();
        }
    }

    private void initialise() {
        assert state == INIT;
        state = INITIALISING;
        prepare();
        rootNode = createRootNode();
        state = READY;
        pull();
    }

    protected abstract void prepare();

    abstract ROOTNODE createRootNode();

    synchronized void pull() {
        assert state == READY;
        state = PULLING;
        readNextAnswer();
    }

    abstract void readNextAnswer();

    @Override
    public synchronized void finish() {
        // note: root resolver calls this single-threaded, so is thread safe
        LOG.trace("All answers found.");
        if (state != FINISHED && state != EXCEPTION) {
            if (queue == null) {
                assert state != PULLING;
                assert requiredAnswers.get() == 0;
            } else {
                requiredAnswers.set(0);
                queue.done();
            }
        }
    }

    @Override
    public synchronized void exception(Throwable e) {
        LOG.error("ReasonerProducer exception called with exception: ", e);
        if (state != FINISHED && state != EXCEPTION) {
            exception = e;
            if (queue == null) {
                assert state != PULLING;
                assert requiredAnswers.get() == 0;
            } else {
                requiredAnswers.set(0);
                queue.done(e.getCause());
            }
        }
        throw TypeDBException.of(e);
    }

    @Override
    public synchronized void receiveAnswer(ConceptMap answer) {
        state = READY;
        queue.put(transformAnswer(answer));
        if (requiredAnswers.decrementAndGet() > 0) pull();
    }

    protected abstract ANSWER transformAnswer(ConceptMap answer);

    @Override
    public void recycle() {

    }

    public static class Basic extends ReasonerProducerV4<Basic.RootNode, ConceptMap> {

        private final ResolvableConjunction conjunction;
        private final Modifiers.Filter filter;
        private AtomicInteger answersReceived;

        public Basic(ResolvableConjunction conjunction, Modifiers.Filter filter, Options.Query options, NodeRegistry nodeRegistry, ExplainablesManager explainablesManager) {
            super(options, nodeRegistry, explainablesManager);
            this.conjunction = conjunction;
            this.filter = filter;
            this.answersReceived = new AtomicInteger(0);
        }

        @Override
        protected void prepare() {
            nodeRegistry.prepare(conjunction, new ConceptMap(), filter);
        }

        @Override
        RootNode createRootNode() {
            return nodeRegistry.createRoot(nodeDriver -> new RootNode(nodeRegistry, nodeDriver));
        }

        @Override
        protected ConceptMap transformAnswer(ConceptMap answer) {
            return answer;
        }

        void readNextAnswer() {
            int nextAnswerIndex = answersReceived.getAndIncrement();
            rootNode.driver().execute(rootNode -> rootNode.readAnswerAt(null, nextAnswerIndex));
        }

        class RootNode extends ActorNode<RootNode> {

            private final NodeRegistry.SubRegistry<?,?> subRegistry;
            private ActorNode.Port port;

            protected RootNode(NodeRegistry nodeRegistry, Driver<RootNode> driver) {
                super(nodeRegistry, driver, () -> "RootNode: " + conjunction.pattern());
                ConjunctionController.ConjunctionStreamPlan csPlan = nodeRegistry.conjunctionStreamPlan(conjunction, new ConceptMap());
                this.subRegistry = nodeRegistry.getRegistry(csPlan);
                this.port = null;
            }

            @Override
            public void initialise() {
                port = createPort(subRegistry.getNode(new ConceptMap()));
                nodeRegistry.perfCounters().startPeriodicPrinting();
            }

            @Override
            public void terminate(Throwable e) {
                nodeRegistry.perfCounters().stopPrinting();
                super.terminate(e);
                Basic.this.exception(e);
            }

            @Override
            public void readAnswerAt(ActorNode.Port reader, int index) {
                assert index == port.lastRequestedIndex() + 1;
                propagatePull(reader, index);
            }

            @Override
            protected void propagatePull(ActorNode.Port reader, int index) {
                port.readNext();
            }


            @Override
            protected void handleAnswer(Port onPort, Message.Answer answer) {
                Basic.this.receiveAnswer(answer.answer());
            }

            @Override
            protected void handleDone(Port onPort) {
                Basic.this.finish();
                nodeRegistry.perfCounters().stopPrinting();
            }
        }
    }
}