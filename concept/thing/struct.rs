/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::type_::vertex::TypeVertexEncoding;
use encoding::layout::prefix::Prefix;
use encoding::value::value::Value;
use encoding::value::value_struct::{StructIndexEntry, StructIndexEntryKey};
use lending_iterator::{LendingIterator, Peekable, Seekable};
use resource::constants::encoding::StructFieldIDUInt;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use resource::profile::StorageCounters;
use storage::key_range::KeyRange;
use storage::key_value::StorageKey;
use storage::snapshot::iterator::SnapshotRangeIterator;
use storage::snapshot::ReadableSnapshot;

use crate::error::ConceptReadError;
use crate::thing::attribute::Attribute;
use crate::thing::ThingAPI;
use crate::type_::attribute_type::AttributeType;

pub struct StructIndexForAttributeTypeIterator {
    prefix: StorageKey<'static, { BUFFER_KEY_INLINE }>,
    iterator: SnapshotRangeIterator,
}

impl StructIndexForAttributeTypeIterator {
   pub(crate) fn new(
       snapshot: &impl ReadableSnapshot,
       vertex_generator: &ThingVertexGenerator,
       attribute_type: AttributeType,
       path_to_field: &[StructFieldIDUInt],
       value: Value<'_>,
   ) -> Result<Self, Box<ConceptReadError>> {
       let prefix = StructIndexEntry::build_prefix_typeid_path_value(
           snapshot,
           vertex_generator,
           path_to_field,
           &value,
           &attribute_type.vertex(),
       )
           .map_err(|source| Box::new(ConceptReadError::SnapshotIterate { source }))?;
       let iterator = snapshot
           .iterate_range(&KeyRange::new_within(prefix.clone(), Prefix::IndexValueToStruct.fixed_width_keys()), StorageCounters::DISABLED);
       Ok(Self { prefix, iterator } )
   }
}

impl LendingIterator for StructIndexForAttributeTypeIterator {
    type Item<'a> = Result<Attribute, Box<ConceptReadError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator
            .next()
            .map(|result| {
                result.map(|(key, _)| {
                    Attribute::new(
                        StructIndexEntry::new(StructIndexEntryKey::new(key.into_bytes()), None).attribute_vertex(),
                    )
                })
                    .map_err(|err| Box::new(ConceptReadError::SnapshotIterate { source: err }))
            })
    }
}

impl Seekable<Attribute> for Peekable<StructIndexForAttributeTypeIterator> {
    fn seek(&mut self, target: &Attribute) {
        // can we guarantee that the PATH is complete and that we will therefore generate in-order attributes?
        // use simple looping seek for now...
        while let Some(Ok(attribute)) = self.peek() {
            if attribute.cmp(target) == Ordering::Less {
                continue
            } else {
                break;
            }
        }
    }

    fn compare_key(&self, attribute: &Self::Item<'_>, other_attribute: &Attribute) -> Ordering {
        if let Ok(attribute) = attribute {
            attribute.cmp(other_attribute)
        } else {
            // arbitrarily choose equal
            Ordering::Equal
        }
    }
}
