/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use encoding::graph::thing::vertex::AttributeVertex;

use crate::ConceptAPI;
use crate::thing::{AttributeAPI, ThingAPI};
use crate::type_::TypeAPI;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attribute<'a> {
    vertex: AttributeVertex<'a>,
}

impl<'a> Attribute<'a> {
    fn new(vertex: AttributeVertex<'a>) -> Self {
        Attribute { vertex: vertex }
    }
}

impl<'a> ThingAPI<'a> for Attribute<'a> {}

impl<'a> ConceptAPI<'a> for Attribute<'a> {}

impl<'a> AttributeAPI<'a> for Attribute<'a> {
    fn vertex(&'a self) -> &AttributeVertex<'a> {
        &self.vertex
    }

    fn into_owned(self) -> Attribute<'static> {
        Attribute { vertex: self.vertex.into_owned() }
    }
}
