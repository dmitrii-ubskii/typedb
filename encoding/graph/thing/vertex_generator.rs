/*
 * Copyright (C) 2023 Vaticle
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


use std::sync::atomic::{AtomicU64, Ordering};

use crate::graph::thing::vertex::{ObjectNumber, ObjectVertex};
use crate::graph::type_::vertex::{TypeID, TypeIDUInt};
use crate::layout::prefix::PrefixType;

pub struct ThingVertexGenerator {
    entity_numbers: Box<[AtomicU64]>,
    relation_numbers: Box<[AtomicU64]>,
    attribute_numbers: Box<[AtomicU64]>,
}

impl ThingVertexGenerator {
    pub fn new() -> ThingVertexGenerator {
        // TODO: we should create a resizable Vector linked to the number of types/highest id of each type
        //       this will speed up booting time on load (loading this will require MAX types * 3 iterator searches) and reduce memory footprint
        ThingVertexGenerator {
            entity_numbers: (0..TypeIDUInt::MAX as usize)
                .map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice(),
            relation_numbers: (0..TypeIDUInt::MAX as usize)
                .map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice(),
            attribute_numbers: (0..TypeIDUInt::MAX as usize)
                .map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice(),
        }
    }

    pub fn load() -> ThingVertexGenerator {
        todo!()
    }

    pub fn take_entity_vertex(&self, type_number: &TypeID<'_>) -> ObjectVertex<'static> {
        let index = type_number.as_u16() as usize;
        let entity_number = self.entity_numbers[index].fetch_add(1, Ordering::Relaxed);
        ObjectVertex::build(&PrefixType::VertexEntity.prefix(), type_number, ObjectNumber::build(entity_number))
    }
}