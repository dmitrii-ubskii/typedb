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
 *
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ExplainableController extends AbstractController<Pair<Concludable, ConceptMap>, ConceptMap, Explanation, ExplainableController.Processor.Request, ExplainableController.Processor, ExplainableController> {

    private final Map<Rule.Condition.ConditionBranch, Driver<NestedConjunctionController>> conditionBodyControllers;

    public ExplainableController(Driver<ExplainableController> driver, Context controllerContext, ReasonerConsumer<Explanation> reasonerConsumer) {
        super(driver, controllerContext, () -> ExplainableController.class.getSimpleName());
        conditionBodyControllers = new HashMap<>();
    }

    @Override
    protected void setUpUpstreamControllers() {
        registry().logicManager().rules().flatMap(rule -> iterate(rule.condition().branches()))
                .forEachRemaining(branch -> {
                    Driver<NestedConjunctionController> controller = registry().createNestedConjunction(branch.conjunction(), branch.conjunction().pattern().retrieves());
                    conditionBodyControllers.put(branch, controller);
                });
    }

    @Override
    public void routeConnectionRequest(Processor.Request connectionRequest) {
        TODO
    }

    @Override
    protected Processor createProcessorFromDriver(Driver<Processor> processorDriver, Pair<Concludable, ConceptMap> boundedConcludable) {
        return new Processor(boundedConcludable, processorDriver, driver(), processorContext(),
                () -> RootDisjunctionController.Processor.class.getSimpleName() + "(explainable:" + boundedConcludable.first() + "::" + boundedConcludable.second() + ")");
    }


    public static class Processor extends AbstractProcessor<ConceptMap, Explanation, Processor.Request, Processor> {
        private final LogicManager logicMgr;
        private final Concludable concludable;
        private final ConceptMap bounds;

        protected Processor(Pair<Concludable, ConceptMap> boundConcludable, Driver<Processor> driver, Driver<ExplainableController> controller, Context context, Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.logicMgr = controller.actor().registry().logicManager();
            this.concludable = boundConcludable.first();
            this.bounds = boundConcludable.second();
        }

        @Override
        public void setUp() {
            PoolingStream<Explanation> fanIn = new PoolingStream.BufferStream<>(this);
            setHubReactive(fanIn);
            Map<Rule, Set<Unifier>> unifiers = logicMgr.applicableRules(concludable);
            for (Map.Entry<Rule, Set<Unifier>> ruleUnifier : unifiers.entrySet()) {
                Set<ConceptMap> mappedBounds = iterate(ruleUnifier.getValue())
                        .map(unifier -> unifier.unify(bounds).get().first().filter(ruleUnifier.getKey().conclusion().retrievableIds()))
                        .toSet();
                for (Rule.Condition.ConditionBranch branch : ruleUnifier.getKey().condition().branches()) {
                    InputPort<ConceptMap> input = createInputPort();
                    input.map(conceptMap -> toExplanation(branch, conceptMap)).registerSubscriber(fanIn);
                    mappedBounds.forEach(mappedBound -> {
                        requestConnection(new Processor.Request(
                                input.identifier(), driver(), branch.conjunction(), mappedBound
                        ));
                    });
                }
            }
        }

        private Explanation toExplanation(Rule.Condition.ConditionBranch branch, ConceptMap conceptMap) {
            TODO
        }

        static class Request extends AbstractRequest<ResolvableConjunction, ConceptMap, ConceptMap> {

            Request(
                    Reactive.Identifier inputPortId, Driver<? extends Processor> inputPortProcessor,
                    ResolvableConjunction controllerId, ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

        }
    }
}
