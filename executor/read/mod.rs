/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{iter::Peekable, sync::Arc};

use compiler::executable::{
    match_::planner::{function_plan::ExecutableFunctionRegistry, match_executable::MatchExecutable},
    pipeline::ExecutableStage,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::{
    read::{
        pattern_executor::{BranchIndex, ExecutorIndex, PatternExecutor},
        step_executor::create_executors_for_pipeline_stages,
        tabled_call_executor::TabledCallExecutor,
        tabled_functions::TableIndex,
    },
    row::MaybeOwnedRow,
};

mod collecting_stage_executor;
pub(super) mod control_instruction;
pub mod expression_executor;
mod immediate_executor;
mod nested_pattern_executor;
pub(crate) mod pattern_executor;
pub(crate) mod step_executor;
mod stream_modifier;
pub(crate) mod tabled_call_executor;
pub mod tabled_functions;

// And use the below one instead
pub(super) fn TODO_REMOVE_create_executors_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    match_executable: &MatchExecutable,
) -> Result<PatternExecutor, ConceptReadError> {
    let executors =
        step_executor::create_executors_for_match(snapshot, thing_manager, function_registry, match_executable)?;
    Ok(PatternExecutor::new(executors))
}

pub(super) fn create_executors_for_pipeline(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    executable_stages: &Vec<ExecutableStage>,
) -> Result<PatternExecutor, ConceptReadError> {
    let executors = create_executors_for_pipeline_stages(
        snapshot,
        thing_manager,
        function_registry,
        executable_stages,
        executable_stages.len() - 1,
    )?;
    Ok(PatternExecutor::new(executors))
}

#[derive(Debug)]
pub(crate) enum SuspendPoint {
    TabledCall(TabledCallSuspension),
    Nested(NestedSuspension),
}

impl SuspendPoint {
    fn depth(&self) -> usize {
        match self {
            SuspendPoint::TabledCall(tabled_call) => tabled_call.depth,
            SuspendPoint::Nested(nested) => nested.depth,
        }
    }
}

#[derive(Debug)]
pub(super) struct TabledCallSuspension {
    pub(crate) executor_index: ExecutorIndex,
    pub(crate) depth: usize,
    pub(crate) input_row: MaybeOwnedRow<'static>,
    pub(crate) next_table_row: TableIndex,
}

#[derive(Debug)]
pub(super) struct NestedSuspension {
    pub(crate) executor_index: ExecutorIndex,
    pub(crate) depth: usize,
    pub(crate) branch_index: BranchIndex,
    pub(crate) input_row: MaybeOwnedRow<'static>,
}

#[derive(Debug)]
pub(super) struct SuspendPointContext {
    at_depth: usize,
    suspended_points: Vec<SuspendPoint>,
    restore_from: Peekable<std::vec::IntoIter<SuspendPoint>>,
}

#[derive(Debug, PartialEq, Eq)]
struct SuspendPointTrackerState(usize);

impl SuspendPointContext {
    pub(crate) fn new() -> Self {
        Self { at_depth: 0, suspended_points: Vec::new(), restore_from: Vec::new().into_iter().peekable() }
    }

    pub(crate) fn swap_suspend_and_restore_points(&mut self) {
        debug_assert!(self.restore_from.peek().is_none());
        let mut tmp = Vec::new();
        std::mem::swap(&mut tmp, &mut self.suspended_points);
        self.restore_from = tmp.into_iter().peekable();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.suspended_points.is_empty()
    }

    pub(crate) fn current_depth(&self) -> usize {
        self.at_depth
    }

    fn record_nested_pattern_entry(&mut self) -> SuspendPointTrackerState {
        self.at_depth += 1;
        SuspendPointTrackerState(self.suspended_points.len())
    }

    fn record_nested_pattern_exit(&mut self) -> SuspendPointTrackerState {
        self.at_depth -= 1;
        SuspendPointTrackerState(self.suspended_points.len())
    }

    fn push_nested(
        &mut self,
        executor_index: ExecutorIndex,
        branch_index: BranchIndex,
        input_row: MaybeOwnedRow<'static>,
    ) {
        self.suspended_points.push(SuspendPoint::Nested(NestedSuspension {
            depth: self.at_depth,
            executor_index,
            branch_index,
            input_row,
        }))
    }

    fn push_tabled_call(&mut self, executor_index: ExecutorIndex, tabled_call_executor: &TabledCallExecutor) {
        self.suspended_points.push(tabled_call_executor.create_suspend_point_for(executor_index, self.at_depth))
    }

    fn next_restore_point_from_current_depth(&mut self) -> Option<SuspendPoint> {
        let has_next = if let Some(point) = self.restore_from.peek() { point.depth() == self.at_depth } else { false };
        if has_next {
            self.restore_from.next()
        } else {
            None
        }
    }
}
