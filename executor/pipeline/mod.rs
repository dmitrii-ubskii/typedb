/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    sync::Arc,
};

use compiler::VariablePosition;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use error::typedb_error;
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{batch::Batch, row::MaybeOwnedRow, write::WriteError};

pub mod delete;
pub mod initial;
pub mod insert;
pub mod match_;
pub mod stage;

pub trait StageAPI<Snapshot: ReadableSnapshot + 'static>: 'static {
    type OutputIterator: StageIterator;

    fn named_selected_outputs(&self) -> HashMap<VariablePosition, String>;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>), (Arc<Snapshot>, PipelineExecutionError)>;
}

pub trait StageIterator:
    for<'a> LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>> + Sized
{
    fn collect_owned(mut self) -> Result<Batch, PipelineExecutionError> {
        // specific iterators can optimise this by not iterating + collecting!
        let first = self.next();
        let mut batch = match first {
            None => return Ok(Batch::new(0, 1)),
            Some(row) => {
                let row = row?;
                let mut batch = Batch::new(row.len() as u32, 10);
                batch.append(row);
                batch
            }
        };
        while let Some(row) = self.next() {
            let row = row?;
            batch.append(row);
        }
        Ok(batch)
    }
}

typedb_error!(
    pub PipelineExecutionError(domain = "Executor", prefix = "EXE") {
        // TODO: migrate to `typedb_error` once they are typedb errors
        ConceptRead(1, "Error reading concept.", ( source: ConceptReadError )),
        InitialisingMatchIterator(2, "Error initialising Match clause iterator.", ( source: ConceptReadError )),
        WriteError(3, "Error executing write operation.", ( source: WriteError )),
    }
);

// Can be used as normal lending iterator, or optimally collect into owned using `collect_owned()`
pub struct WrittenRowsIterator {
    rows: Batch,
    index: usize,
}

impl WrittenRowsIterator {
    pub(crate) fn new(rows: Batch) -> Self {
        Self { rows, index: 0 }
    }
}

impl LendingIterator for WrittenRowsIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, PipelineExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let index = self.index;
        if index < self.rows.len() {
            self.index += 1;
            Some(Ok(self.rows.get_row(index)))
        } else {
            return None;
        }
    }
}

impl StageIterator for WrittenRowsIterator {
    fn collect_owned(self) -> Result<Batch, PipelineExecutionError> {
        debug_assert!(self.index == 0, "Truncating start of rows is not implemented");
        Ok(self.rows)
    }
}