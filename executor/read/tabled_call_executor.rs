/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    fmt,
    sync::{MutexGuard, TryLockError},
};

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use ir::pipeline::function_signature::FunctionID;

use crate::{
    batch::FixedBatch,
    read::{
        suspension::{PatternSuspension, TabledCallSuspension},
        tabled_call_executor::TabledCallResult::Suspend,
        tabled_functions::{CallKey, TableIndex, TabledFunctionPatternExecutorState, TabledFunctionState},
        ExecutorIndex,
    },
    row::MaybeOwnedRow,
};

pub(crate) struct TabledCallExecutor {
    function_id: FunctionID,
    argument_positions: Vec<VariablePosition>,
    assignment_positions: Vec<Option<VariablePosition>>,
    output_width: u32
}

impl fmt::Debug for TabledCallExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TabledCallExecutor (function id {:?})", self.function_id)
    }
}

#[derive(Debug)]
pub struct TabledCallExecutorState {
    pub(crate) call_key: CallKey,
    pub(crate) input: MaybeOwnedRow<'static>,
    pub(crate) next_table_row: TableIndex,
    pub(crate) last_seen_scc_total_table_size: Option<usize>, // Used to detect termination
}

impl TabledCallExecutorState {
    pub(crate) fn add_batch_to_table(&mut self, state: &TabledFunctionState, batch: FixedBatch) -> FixedBatch {
        let deduplicated_batch = state.add_batch_to_table(batch);
        *self.next_table_row += deduplicated_batch.len() as usize;
        deduplicated_batch
    }

    pub(crate) fn active_call_key(&self) -> &CallKey {
        &self.call_key
    }

    pub(crate) fn create_suspension_at(&self, executor_index: ExecutorIndex, depth: usize) -> PatternSuspension {
        PatternSuspension::AtTabledCall(TabledCallSuspension {
            executor_index,
            depth,
            input_row: self.input.clone().into_owned(),
            next_table_row: self.next_table_row,
        })
    }

    pub(crate) fn try_read_next_batch<'a>(
        &mut self,
        tabled_function_state: &'a TabledFunctionState,
    ) -> TabledCallResult<'a> {
        // Maybe return a batch?
        let table_read = tabled_function_state.table.read().unwrap();
        if *self.next_table_row < table_read.len() {
            let batch = table_read.read_batch_starting(self.next_table_row);
            *self.next_table_row += batch.len() as usize;
            TabledCallResult::RetrievedFromTable(batch)
        } else {
            drop(table_read);
            match tabled_function_state.executor_state.try_lock() {
                Ok(executor_mutex_guard) => TabledCallResult::MustExecutePattern(executor_mutex_guard),
                Err(TryLockError::WouldBlock) => Suspend,
                Err(TryLockError::Poisoned(_)) => panic!("The mutex on a tabled function was poisoned"),
            }
        }
    }
}

pub(super) enum TabledCallResult<'a> {
    RetrievedFromTable(FixedBatch),
    MustExecutePattern(MutexGuard<'a, TabledFunctionPatternExecutorState>),
    Suspend,
}

impl TabledCallExecutor {
    pub(crate) fn new(
        function_id: FunctionID,
        argument_positions: Vec<VariablePosition>,
        assignment_positions: Vec<Option<VariablePosition>>,
        output_width: u32,
    ) -> Self {
        Self { function_id, argument_positions, assignment_positions, output_width }
    }

    pub(crate) fn create_fresh_state(&mut self, input: MaybeOwnedRow<'static>) -> TabledCallExecutorState{
        self.create_state(input, TableIndex(0))
    }

    pub(crate) fn restore_from_suspension(&mut self, input: MaybeOwnedRow<'static>, next_table_index: TableIndex) -> TabledCallExecutorState {
        self.create_state(input, next_table_index)
    }

    fn create_state(&mut self, input: MaybeOwnedRow<'static>, next_table_row: TableIndex) -> TabledCallExecutorState {
        let arguments = MaybeOwnedRow::new_owned(
            self.argument_positions.iter().map(|pos| input.get(*pos).to_owned()).collect(),
            input.multiplicity(),
            input.provenance(),
        );
        let call_key = CallKey { function_id: self.function_id.clone(), arguments };
        TabledCallExecutorState { call_key, input, next_table_row, last_seen_scc_total_table_size: None }
    }

    pub(crate) fn map_output(&self, input: MaybeOwnedRow<'_>, returned_batch: FixedBatch) -> FixedBatch {
        let mut output_batch = FixedBatch::new(self.output_width);
        let check_indices: Vec<_> = self
            .assignment_positions
            .iter()
            .enumerate()
            .filter_map(|(src, &dst)| Some((VariablePosition::new(src as u32), dst?)))
            .filter(|(_, dst)| dst.as_usize() < input.len() && input.get(*dst) != &VariableValue::Empty)
            .collect(); // TODO: Can we move this to compilation?

        for return_index in 0..returned_batch.len() {
            // TODO: Deduplicate?
            let returned_row = returned_batch.get_row(return_index);
            if check_indices.iter().all(|(src, dst)| returned_row.get(*src) == input.get(*dst)) {
                output_batch.append(|mut output_row| {
                    output_row.copy_from_row(input.as_reference());
                    output_row.copy_mapped(
                        returned_row,
                        self.assignment_positions
                            .iter()
                            .enumerate()
                            .filter_map(|(src, &dst)| Some((VariablePosition::new(src as u32), dst?))),
                    );
                });
            }
        }
        output_batch
    }
}
