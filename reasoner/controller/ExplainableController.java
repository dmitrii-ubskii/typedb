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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;

public class ExplainableController extends AbstractController<ConceptMap, ConceptMap, Explanation, DisjunctionController.Processor.Request, ExplainableController.Processor, ExplainableController> {
    public ExplainableController(Driver<ExplainableController> driver, Concludable concludable, ConceptMap bounds, Context controllerContext, ReasonerConsumer<Explanation> reasonerConsumer) {
        super(driver, controllerContext,  () -> ExplainableController.class.getSimpleName() + "(concludable:" + concludable + "; bounds: " + bounds + ")");
    }

    public static class Processor extends AbstractProcessor<ConceptMap, Explanation, Processor.Request, Processor> {

        static class Request extends AbstractRequest<ResolvableConjunction, ConceptMap, ConceptMap> {

            Request(
                    Reactive.Identifier inputPortId, Driver<? extends DisjunctionController.Processor<?, ?>> inputPortProcessor,
                    ResolvableConjunction controllerId, ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

        }
    }
}
