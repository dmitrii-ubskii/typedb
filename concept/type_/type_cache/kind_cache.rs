/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use encoding::value::label::Label;
use encoding::value::value_type::ValueType;
use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::Ordering;
use crate::type_::owns::{Owns, OwnsAnnotation};
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::KindAPI;

#[derive(Debug)]
pub(crate) struct EntityTypeCache {
    pub(super) common_type_cache: CommonTypeCache<EntityType<'static>>,
    pub(super) owner_player_cache: OwnerPlayerCache,
    // ...
}

#[derive(Debug)]
pub(crate) struct RelationTypeCache {
    pub(super) common_type_cache: CommonTypeCache<RelationType<'static>>,
    pub(super) relates_declared: HashSet<Relates<'static>>,
    pub(super) owner_player_cache: OwnerPlayerCache,
}

#[derive(Debug)]
pub(crate) struct RoleTypeCache {
    pub(super) common_type_cache: CommonTypeCache<RoleType<'static>>,
    pub(super) ordering: Ordering,
    pub(super) relates_declared: Relates<'static>,
}

#[derive(Debug)]
pub(crate) struct AttributeTypeCache {
    pub(super) common_type_cache: CommonTypeCache<AttributeType<'static>>,
    pub(super) value_type: Option<ValueType>,
    // owners: HashSet<Owns<'static>>
}

#[derive(Debug)]
pub(crate) struct OwnsCache {
    pub(super) ordering: Ordering,
    pub(super) annotations_declared: HashSet<OwnsAnnotation>,
}

#[derive(Debug)]
pub(crate) struct CommonTypeCache<T: KindAPI<'static>> {
    pub(super) type_: T,
    pub(super) label: Label<'static>,
    pub(super) is_root: bool,
    pub(super) annotations_declared: HashSet<T::AnnotationType>,
    // TODO: Should these all be sets instead of vec?
    pub(super) supertype: Option<T>, // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) supertypes: Vec<T>,   // TODO: use smallvec if we want to have some inline - benchmark.
    pub(super) subtypes_declared: Vec<T>, // TODO: benchmark smallvec.
    pub(super) subtypes_transitive: Vec<T>, // TODO: benchmark smallvec
}

#[derive(Debug)]
pub struct OwnerPlayerCache {
    pub(super) owns_declared: HashSet<Owns<'static>>,
    pub(super) plays_declared: HashSet<Plays<'static>>,
}
