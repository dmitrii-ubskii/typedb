package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.RootSink;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.controller.ConjunctionController.merge;
import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanInFanOut;

public class RootExplainableController extends
        AbstractController<Pair<Concludable, ConceptMap>, Explanation, Explanation, RootExplainableController.Processor.Request, RootExplainableController.Processor, RootExplainableController> {
    private final ReasonerConsumer<Explanation> reasonerConsumer;
    private final Pair<Concludable, ConceptMap> boundConcludable;
    private Driver<ExplainableController> explainableController;

    public RootExplainableController(Pair<Concludable, ConceptMap> boundConcludable, Driver<RootExplainableController> driver, Context controllerContext, ReasonerConsumer<Explanation> reasonerConsumer) {
        super(driver, controllerContext, () -> ExplainableController.class.getSimpleName() + "(concludable: " + boundConcludable.first() + ", bounds: " + boundConcludable +  ")");
        this.boundConcludable = boundConcludable;
        this.reasonerConsumer = reasonerConsumer;
    }

    @Override
    protected void setUpUpstreamControllers() {
        explainableController = registry().getOrCreateExplainableController();
    }

    @Override
    public void routeConnectionRequest(Processor.Request req) {
        explainableController.execute(actor -> actor.establishProcessorConnection(req));
    }

    @Override
    protected Processor createProcessorFromDriver(Driver<Processor> processorDriver, Pair<Concludable, ConceptMap> concludableConceptMapPair) {
        return new Processor(boundConcludable, reasonerConsumer, processorDriver, driver(), processorContext(),
                () -> Processor.class.getSimpleName() + "(concludable:" + boundConcludable.first() + ", bounds: " + boundConcludable.second() + ")");
    }

    public static class Processor extends AbstractProcessor<Explanation, Explanation, RootExplainableController.Processor.Request, Processor> {

        private final Pair<Concludable, ConceptMap> boundConcludable;
        private final ReasonerConsumer<Explanation> reasonerConsumer;
        private RootSink<Explanation> rootSink;

        protected Processor(Pair<Concludable, ConceptMap> boundConcludable, ReasonerConsumer<Explanation> reasonerConsumer,
                            Driver<Processor> driver, Driver<RootExplainableController> controller, Context context, Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.boundConcludable = boundConcludable;
            this.reasonerConsumer = reasonerConsumer;
        }


        @Override
        public void setUp() {
            setHubReactive(fanInFanOut(this));
            rootSink = new RootSink<>(this, reasonerConsumer);
            hubReactive().registerSubscriber(rootSink);

            InputPort<Explanation> input = createInputPort();
            requestConnection(new Request(input.identifier(), driver(), null, boundConcludable));
        }

        @Override
        public void rootPull() {
            rootSink.pull();
        }

        @Override
        public void onFinished(Reactive.Identifier finishable) {
            assert finishable == rootSink.identifier();
            rootSink.finished();
        }

        public static class Request extends AbstractRequest<Void, Pair<Concludable, ConceptMap>, Explanation> {
            protected Request(Reactive.Identifier inputPortId, Driver<Processor> inputPortProcessor, Void unused, Pair<Concludable, ConceptMap> boundConcludable) {
                super(inputPortId, inputPortProcessor, unused, boundConcludable);
            }
        }
    }


}
