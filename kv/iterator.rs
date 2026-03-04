/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use bytes::byte_array::ByteArray;
use lending_iterator::{LendingIterator, Seekable};
use resource::constants::kv::ITERATOR_CONTINUE_CONDITION_INLINE;

use crate::{memory::iterator::InMemoryRangeIterator, rocks::iterator::RocksRangeIterator, KVStoreError};

pub type KVIteratorItem<'a> = Result<(&'a [u8], &'a [u8]), Box<dyn KVStoreError>>;

pub enum KVRangeIterator {
    RocksDB(RocksRangeIterator),
    InMemory(InMemoryRangeIterator),
}

impl LendingIterator for KVRangeIterator {
    type Item<'a> = KVIteratorItem<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            Self::RocksDB(iter) => iter.next(),
            Self::InMemory(iter) => iter.next(),
        }
    }
}

impl Seekable<[u8]> for KVRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        match self {
            Self::RocksDB(iter) => iter.seek(key),
            Self::InMemory(iter) => iter.seek(key),
        }
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        match self {
            Self::RocksDB(iter) => iter.compare_key(item, key),
            Self::InMemory(iter) => iter.compare_key(item, key),
        }
    }
}

pub(crate) enum ContinueCondition {
    ExactPrefix(ByteArray<{ ITERATOR_CONTINUE_CONDITION_INLINE }>),
    EndPrefixInclusive(ByteArray<{ ITERATOR_CONTINUE_CONDITION_INLINE }>),
    EndPrefixExclusive(ByteArray<{ ITERATOR_CONTINUE_CONDITION_INLINE }>),
    Always,
}

pub(crate) fn accept_value<E>(condition: &ContinueCondition, value: &Result<(&[u8], &[u8]), E>) -> bool {
    match value {
        Ok((key, _)) => match condition {
            ContinueCondition::ExactPrefix(prefix) => key.starts_with(prefix),
            ContinueCondition::EndPrefixInclusive(end_inclusive) => {
                // pass to Rust's lexicographical byte comparison
                *key <= &**end_inclusive
            }
            ContinueCondition::EndPrefixExclusive(end_exclusive) => {
                // pass to Rust's lexicographical byte comparison
                *key < &**end_exclusive
            }
            ContinueCondition::Always => true,
        },
        Err(_) => true,
    }
}

pub(crate) fn compare_key<E>(item: &Result<(&[u8], &[u8]), E>, key: &[u8]) -> Ordering {
    if let Ok((peek, _)) = item {
        peek.cmp(&key)
    } else {
        Ordering::Equal
    }
}
