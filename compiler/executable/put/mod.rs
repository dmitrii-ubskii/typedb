/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeSet, HashMap, HashSet};

use answer::variable::Variable;

use crate::{
    executable::{
        insert::executable::InsertExecutable, match_::planner::match_executable::MatchExecutable, next_executable_id,
    },
    VariablePosition,
};
use crate::executable::insert::instructions::ConnectionInstruction;
use crate::executable::insert::TypeSource;

#[derive(Debug)]
pub struct PutExecutable {
    pub executable_id: u64,
    pub match_: MatchExecutable,
    pub insert: InsertExecutable,
}

impl PutExecutable {
    pub(crate) fn new(match_: MatchExecutable, insert: InsertExecutable) -> PutExecutable {
        debug_assert!({
            let match_positions = match_
                .variable_positions()
                .clone()
                .into_iter()
                .filter(|(_, pos)| match_.selected_variables().contains(pos))
                .collect::<HashMap<_, _>>();
            match_positions.iter().all(|(v, pos)| insert.output_row_schema[pos.as_usize()].unwrap().0 == *v)
        });
        Self { executable_id: next_executable_id(), match_, insert }
    }

    pub(crate) fn output_row_mapping(&self) -> &HashMap<Variable, VariablePosition> {
        self.match_.variable_positions()
    }

    pub fn output_width(&self) -> usize {
        self.insert.output_width()
    }

    pub fn referenced_input_positions(&self) -> HashSet<VariablePosition> {
        let mut positions = HashSet::with_capacity(self.insert.connection_instructions.len() * 3 + self.insert.concept_instructions.len());
        self.insert.connection_instructions.iter().for_each(|c| match c {
            ConnectionInstruction::Has(has) => positions.extend([has.owner.0, has.attribute.0].into_iter()),
            ConnectionInstruction::Links(links) => {
                positions.extend([links.relation.0, links.player.0].into_iter());
                if let TypeSource::InputVariable(p) = &links.role {
                    positions.insert(VariablePosition::new(p.position));
                }
            },
        });
        self.insert.concept_instructions.iter().for_each(|c| {
            if let TypeSource::InputVariable(p) = &c.inserted_type() {
                positions.insert(VariablePosition::new(p.position));
            }
        });
        positions
    }

    pub fn inserted_positions(&self) -> Vec<VariablePosition> {
        self.insert.concept_instructions.iter().map(|c| c.inserted_position().0).collect()
    }
}
