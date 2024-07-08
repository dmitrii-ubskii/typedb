/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::inference::type_inference::TypeAnnotations;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{
        function_executor::FunctionExecutor,
        pattern_executor::{ImmutableRow, PatternExecutor, Row},
    },
    planner::program_plan::ProgramPlan,
};

pub struct ProgramExecutor {
    entry: PatternExecutor,
    functions: HashMap<DefinitionKey<'static>, FunctionExecutor>,
}

impl ProgramExecutor {
    pub fn new<Snapshot: ReadableSnapshot>(
        program_plan: ProgramPlan,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
    ) -> Result<Self, ConceptReadError> {
        let ProgramPlan { entry: entry_plan, functions: function_plans } = program_plan;
        let entry = PatternExecutor::new(entry_plan, type_annotations, snapshot, thing_manager)?;

        // TODO: functions

        Ok(Self { entry: entry, functions: HashMap::new() })
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        snapshot: Arc<Snapshot>,
        thing_manager: Arc<ThingManager>,
    ) -> impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>> {
        self.entry.into_iterator(snapshot, thing_manager)
    }
}