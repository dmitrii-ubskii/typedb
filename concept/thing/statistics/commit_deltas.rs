/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    fmt,
    hash::Hash,
    ops::Bound,
    time::Instant,
};

use bytes::Bytes;
use durability::DurabilityRecordType;
use encoding::graph::{
    Typed,
    thing::{
        ThingVertex,
        edge::{ThingEdgeHas, ThingEdgeIndexedRelation, ThingEdgeLinks},
        vertex_attribute::AttributeVertex,
        vertex_object::ObjectVertex,
    },
    type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeIDUInt, TypeVertexEncoding},
};
use error::typedb_error;
use resource::{
    constants::{
        database::{STATISTICS_DURABLE_WRITE_CHANGE_COUNT, STATISTICS_DURABLE_WRITE_SEQ_NUMBERS},
        snapshot::BUFFER_KEY_INLINE,
    },
    profile::StorageCounters,
};
use serde::{Deserialize, Serialize};
use storage::{
    MVCCStorage,
    durability_client::{DurabilityClient, DurabilityClientError, DurabilityRecord, UnsequencedDurabilityRecord},
    iterator::MVCCReadError,
    key_value::{StorageKeyArray, StorageKeyReference},
    keyspace::IteratorPool,
    record::{CommitRecord, CommitType},
    recovery::commit_recovery::{RecoveryCommitStatus, StorageRecoveryError, load_commit_data_from_with_context},
    sequence_number::SequenceNumber,
    snapshot::{buffer::OperationsBuffer, write::Write},
};
use tracing::{Level, event};

use crate::{
    thing::{
        ThingAPI, attribute::Attribute, entity::Entity, object::Object, relation::Relation, statistics::CommittedWrites,
    },
    type_::{
        TypeAPI, attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType,
        relation_type::RelationType, role_type::RoleType,
    },
};

struct CommitDeltas {
    pub total_delta: i64,

    pub total_thing_delta: i64,
    pub total_entity_delta: i64,
    pub total_relation_delta: i64,
    pub total_attribute_delta: i64,
    pub total_role_delta: i64,
    pub total_has_delta: i64,

    pub entity_deltas: HashMap<EntityType, i64>,
    pub relation_deltas: HashMap<RelationType, i64>,
    pub attribute_deltas: HashMap<AttributeType, i64>,
    pub role_deltas: HashMap<RoleType, i64>,

    pub has_attribute_deltas: HashMap<ObjectType, HashMap<AttributeType, i64>>,
    pub attribute_owner_deltas: HashMap<AttributeType, HashMap<ObjectType, i64>>,
    pub role_player_deltas: HashMap<ObjectType, HashMap<RoleType, i64>>,
    pub relation_role_deltas: HashMap<RelationType, HashMap<RoleType, i64>>,
    pub relation_role_player_deltas: HashMap<RelationType, HashMap<RoleType, HashMap<ObjectType, i64>>>,
    pub player_role_relation_deltas: HashMap<ObjectType, HashMap<RoleType, HashMap<RelationType, i64>>>,

    // TODO: adding role types is possible, but won't help with filtering before reading storage since roles are not in the prefix
    pub links_index_deltas: HashMap<ObjectType, HashMap<ObjectType, i64>>,

    pub total_invisible_delta: u64,

    pub total_invisible_thing_delta: u64,
    pub total_invisible_entity_delta: u64,
    pub total_invisible_relation_delta: u64,
    pub total_invisible_attribute_delta: u64,
    pub total_invisible_role_delta: u64,
    pub total_invisible_has_delta: u64,

    pub invisible_entity_deltas: HashMap<EntityType, u64>,
    pub invisible_relation_deltas: HashMap<RelationType, u64>,
    pub invisible_attribute_deltas: HashMap<AttributeType, u64>,
    pub invisible_role_deltas: HashMap<RoleType, u64>,

    pub invisible_has_attribute_deltas: HashMap<ObjectType, HashMap<AttributeType, u64>>,
    pub invisible_attribute_owner_deltas: HashMap<AttributeType, HashMap<ObjectType, u64>>,
    pub invisible_role_player_deltas: HashMap<ObjectType, HashMap<RoleType, u64>>,
    pub invisible_relation_role_deltas: HashMap<RelationType, HashMap<RoleType, u64>>,
    pub invisible_relation_role_player_deltas: HashMap<RelationType, HashMap<RoleType, HashMap<ObjectType, u64>>>,
    pub invisible_player_role_relation_deltas: HashMap<ObjectType, HashMap<RoleType, HashMap<RelationType, u64>>>,

    // TODO: adding role types is possible, but won't help with filtering before reading storage since roles are not in the prefix
    pub invisible_links_index_deltas: HashMap<ObjectType, HashMap<ObjectType, u64>>,
    // future: attribute value distributions, attribute value ownership distributions, etc.
}

impl CommitDeltas {
    fn from_commit_record<D>(record: CommitRecord) -> Self {
        let writes = CommittedWrites {
            open_sequence_number: record.open_sequence_number(),
            operations: record.into_operations(),
        };
        todo!()
    }

    fn update_write<D>(&mut self, writes: &CommittedWrites) {
        let mut total_delta = 0;
        for (key, write) in writes.operations.iterate_writes() {
            let delta = write_to_delta(&key, &write, writes.open_sequence_number)?;
            if ObjectVertex::is_entity_vertex(StorageKeyReference::from(&key)) {
                let type_ = Entity::new(ObjectVertex::decode(key.bytes())).type_();
                self.update_entities(type_, delta);
                total_delta += delta;
            } else if ObjectVertex::is_relation_vertex(StorageKeyReference::from(&key)) {
                let type_ = Relation::new(ObjectVertex::decode(key.bytes())).type_();
                self.update_relations(type_, delta);
                total_delta += delta;
            } else if AttributeVertex::is_attribute_vertex(StorageKeyReference::from(&key)) {
                let type_ = Attribute::new(AttributeVertex::decode(key.bytes())).type_();
                self.update_attributes(type_, delta);
            } else if ThingEdgeHas::is_has(&key) {
                let edge = ThingEdgeHas::decode(Bytes::Reference(key.bytes()));
                self.update_has(Object::new(edge.from()).type_(), Attribute::new(edge.to()).type_(), delta);
                total_delta += delta;
            } else if ThingEdgeLinks::is_links(&key) {
                let edge = ThingEdgeLinks::decode(Bytes::Reference(key.bytes()));
                let role_type = RoleType::build_from_type_id(edge.role_id());
                self.update_role_player(
                    Object::new(edge.to()).type_(),
                    role_type,
                    Relation::new(edge.from()).type_(),
                    delta,
                );
                total_delta += delta;
            } else if ThingEdgeIndexedRelation::is_index(&key) {
                let edge = ThingEdgeIndexedRelation::decode(Bytes::Reference(key.bytes()));
                self.update_indexed_player(Object::new(edge.from()).type_(), Object::new(edge.to()).type_(), delta);
                // note: don't update total delta based on index
            } else if EntityType::is_decodable_from_key(&key) {
                if matches!(write, Write::Delete) {
                    let type_ = EntityType::read_from(Bytes::Reference(key.bytes()).into_owned());
                    deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                        this.entity_deltas.remove(&type_);
                        this.clear_object_type(ObjectType::Entity(type_));
                    }));
                }
                // note: don't update total delta based on type updates
            } else if RelationType::is_decodable_from_key(&key) {
                if matches!(write, Write::Delete) {
                    let type_ = RelationType::read_from(Bytes::Reference(key.bytes()).into_owned());
                    deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                        this.relation_deltas.remove(&type_);
                        this.relation_role_deltas.remove(&type_);
                        this.clear_object_type(ObjectType::Relation(type_));
                    }));
                }
                // note: don't update total delta based on type updates
            } else if AttributeType::is_decodable_from_key(&key) {
                if matches!(write, Write::Delete) {
                    let type_ = AttributeType::read_from(Bytes::Reference(key.bytes()).into_owned());
                    deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                        this.attribute_deltas.remove(&type_);
                        this.attribute_owner_deltas.remove(&type_);
                        for map in this.has_attribute_deltas.values_mut() {
                            map.remove(&type_);
                        }
                        this.has_attribute_deltas.retain(|_, map| !map.is_empty());
                    }));
                }
                // note: don't update total delta based on type updates
            } else if RoleType::is_decodable_from_key(&key) {
                if matches!(write, Write::Delete) {
                    let type_ = RoleType::read_from(Bytes::Reference(key.bytes()).into_owned());
                    deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                        this.role_deltas.remove(&type_);
                        for map in this.role_player_deltas.values_mut() {
                            map.remove(&type_);
                        }
                        this.role_player_deltas.retain(|_, map| !map.is_empty());
                        for map in this.relation_role_deltas.values_mut() {
                            map.remove(&type_);
                        }
                        this.relation_role_deltas.retain(|_, map| !map.is_empty());
                    }));
                }
                // note: don't update total count based on type updates
            }
        }
    }

    fn saturating_add(delta: &mut i64, update: i64, label: &str) {
        match delta.checked_add(update) {
            Some(value) => *delta = value,
            None => {
                diagnostics::error_with_report!(
                    "Unexpected underflow in statistics {} delta: {} + {}",
                    label,
                    *delta,
                    update
                );
                *delta = 0;
            }
        }
    }

    fn update_entities(&mut self, entity_type: EntityType, update: i64) {
        let delta = self.entity_deltas.entry(entity_type).or_default();
        Self::saturating_add(delta, update, "entity");
        Self::saturating_add(&mut self.total_entity_delta, update, "total_entity");
        Self::saturating_add(&mut self.total_thing_delta, update, "total_thing");
    }

    fn update_relations(&mut self, relation_type: RelationType, update: i64) {
        let delta = self.relation_deltas.entry(relation_type).or_default();
        Self::saturating_add(delta, update, "relation");
        Self::saturating_add(&mut self.total_relation_delta, update, "total_relation");
        Self::saturating_add(&mut self.total_thing_delta, update, "total_thing");
    }

    fn update_attributes(&mut self, attribute_type: AttributeType, update: i64) {
        let delta = self.attribute_deltas.entry(attribute_type).or_default();
        Self::saturating_add(delta, update, "attribute");
        Self::saturating_add(&mut self.total_attribute_delta, update, "total_attribute");
        Self::saturating_add(&mut self.total_thing_delta, update, "total_thing");
    }

    fn update_has(&mut self, owner_type: ObjectType, attribute_type: AttributeType, update: i64) {
        let attribute_delta =
            self.has_attribute_deltas.entry(owner_type).or_default().entry(attribute_type).or_default();
        Self::saturating_add(attribute_delta, update, "has_attribute");
        let owner_delta = self.attribute_owner_deltas.entry(attribute_type).or_default().entry(owner_type).or_default();
        Self::saturating_add(owner_delta, update, "attribute_owner");
        Self::saturating_add(&mut self.total_has_delta, update, "total_has");
    }

    fn update_role_player(
        &mut self,
        player_type: ObjectType,
        role_type: RoleType,
        relation_type: RelationType,
        update: i64,
    ) {
        let role_delta = self.role_deltas.entry(role_type).or_default();
        Self::saturating_add(role_delta, update, "role");
        Self::saturating_add(&mut self.total_role_delta, update, "total_role");
        let role_player_delta = self.role_player_deltas.entry(player_type).or_default().entry(role_type).or_default();
        Self::saturating_add(role_player_delta, update, "role_player");
        let relation_role_delta =
            self.relation_role_deltas.entry(relation_type).or_default().entry(role_type).or_default();
        Self::saturating_add(relation_role_delta, update, "relation_role");
        let relation_role_player_delta = self
            .relation_role_player_deltas
            .entry(relation_type)
            .or_default()
            .entry(role_type)
            .or_default()
            .entry(player_type)
            .or_default();
        Self::saturating_add(relation_role_player_delta, update, "relation_role_player");
        let player_role_relation_delta = self
            .player_role_relation_deltas
            .entry(player_type)
            .or_default()
            .entry(role_type)
            .or_default()
            .entry(relation_type)
            .or_default();
        Self::saturating_add(player_role_relation_delta, update, "player_role_relation");
    }

    fn update_indexed_player(&mut self, player_1_type: ObjectType, player_2_type: ObjectType, update: i64) {
        let player_1_to_2_index_delta =
            self.links_index_deltas.entry(player_1_type).or_default().entry(player_2_type).or_default();
        Self::saturating_add(player_1_to_2_index_delta, update, "player_1_to_2_index");
        if player_1_type != player_2_type {
            let player_2_to_1_index_delta =
                self.links_index_deltas.entry(player_2_type).or_default().entry(player_1_type).or_default();
            Self::saturating_add(player_2_to_1_index_delta, update, "player_2_to_1_index");
        }
    }
}

fn write_to_delta<D>(
    write_key: &StorageKeyArray<{ BUFFER_KEY_INLINE }>,
    write: &Write,
    open_sequence_number: SequenceNumber,
) -> Result<i64, MVCCReadError> {
    let concurrent_commit_range = (Bound::Excluded(open_sequence_number), Bound::Excluded(commit_sequence_number));
    match write {
        Write::Insert { .. } => Ok(1),
        Write::Delete => {
            if commits.range(concurrent_commit_range).any(|(_, writes)| {
                matches!(
                    writes.operations.writes_in(write_key.keyspace_id()).writes_get(write_key.bytes()),
                    Some(Write::Delete)
                )
            }) {
                Ok(0)
            } else {
                Ok(-1)
            }
        }
        Write::Put { reinsert, .. } => {
            // PUT operation which we may have a concurrent commit and may or may not be inserted in the end
            // The easiest way to check whether it was ultimately committed or not is to open the storage at
            // CommitSequenceNumber - 1, and check if it exists. If it exists, we don't count. If it does, we do.
            // However, this induces a read for every PUT, even though 99% of time there is no concurrent put.

            // We only read from storage, if we can't tell from the current set of commits whether a predecessor
            // could have written the same key (open < commits start)

            let first_commit_sequence_number = *commits.first_key_value().unwrap().0;

            if let Some(write) = commits.range(concurrent_commit_range).rev().find_map(|(_, writes)| {
                writes.operations.writes_in(write_key.keyspace_id()).writes_get(write_key.bytes())
            }) {
                match write {
                    Write::Insert { .. } | Write::Put { .. } => Ok(0),
                    Write::Delete => Ok(1),
                }
            } else if open_sequence_number.next() < first_commit_sequence_number {
                if storage
                    .get::<0>(
                        &IteratorPool::new(),
                        write_key,
                        commit_sequence_number.previous(),
                        StorageCounters::DISABLED,
                    )?
                    .is_some()
                {
                    // exists in storage before PUT is committed
                    Ok(0)
                } else {
                    // does not exist in storage before PUT is committed
                    Ok(1)
                }
            } else {
                // no concurrent commit could have occurred - fall back to the flag
                if reinsert.load(std::sync::atomic::Ordering::Relaxed) { Ok(1) } else { Ok(0) }
            }
        }
    }
}
