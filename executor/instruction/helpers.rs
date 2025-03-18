/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use compiler::executable::match_::instructions::VariableModes;
use concept::{
    error::ConceptReadError,
    thing::object::{HasIterator, HasReverseIterator},
};
use itertools::KMergeBy;

use crate::instruction::{
    has_executor::{HasExecutor, HasOrderingFn},
    has_reverse_executor::{ChainedHasReverseIterator, HasReverseExecutor},
    isa_executor,
    isa_executor::{IsaBoundedInner, IsaExecutor},
    isa_reverse_executor,
    isa_reverse_executor::IsaReverseExecutor,
    iterator::{SortedTupleIterator, TupleIterator},
    owns_executor::{OwnsExecutor, OwnsFlattenedVectorInner, OwnsVectorInner},
    owns_reverse_executor::OwnsReverseExecutor,
    plays_executor::{PlaysExecutor, PlaysFlattenedVectorInner, PlaysVectorInner},
    plays_reverse_executor::PlaysReverseExecutor,
    relates_executor::{RelatesExecutor, RelatesFlattenedVectorInner, RelatesVectorInner},
    relates_reverse_executor::RelatesReverseExecutor,
    sub_executor::{SubExecutor, SubVectorInner},
    sub_reverse_executor::SubReverseExecutor,
    tuple::TuplePositions,
    DynamicBinaryIterator, FilterMapUnchangedFn, MapToTupleFn,
};

type ExecutorResult<T> = Result<T, Box<ConceptReadError>>;
pub(super) trait ExecutorIteratorUnbound<Executor: DynamicBinaryIterator>:
    Iterator<Item = ExecutorResult<Executor::Element>> + Sized
{
    fn unbound_into_tuple_iterator(
        self,
        filter_for_row: Box<FilterMapUnchangedFn<Executor::Element>>,
        to_tuple: MapToTupleFn<Executor::Element>,
        tuple_positions: TuplePositions,
        variable_modes: &VariableModes,
    ) -> TupleIterator;
}

pub(super) trait ExecutorIteratorUnboundInverted<Executor: DynamicBinaryIterator>:
    Iterator<Item = ExecutorResult<Executor::Element>> + Sized
{
    fn unbound_inverted_into_tuple_iterator(
        self,
        filter_for_row: Box<FilterMapUnchangedFn<Executor::Element>>,
        to_tuple: MapToTupleFn<Executor::Element>,
        tuple_positions: TuplePositions,
        variable_modes: &VariableModes,
    ) -> TupleIterator;
}

pub(super) trait ExecutorIteratorBoundFrom<Executor: DynamicBinaryIterator>:
    Iterator<Item = ExecutorResult<Executor::Element>> + Sized
{
    fn bound_from_into_tuple_iterator(
        self,
        filter_for_row: Box<FilterMapUnchangedFn<Executor::Element>>,
        to_tuple: MapToTupleFn<Executor::Element>,
        tuple_positions: TuplePositions,
        variable_modes: &VariableModes,
    ) -> TupleIterator;
}

macro_rules! impl_iterator_unbound {
    ($($executor:ty : $iter:ty => $variant:ident,)*) => {
        $(
            impl ExecutorIteratorUnbound<$executor> for $iter {
                fn unbound_into_tuple_iterator(self, filter_for_row: Box<FilterMapUnchangedFn<<$executor as DynamicBinaryIterator>::Element>>, to_tuple: MapToTupleFn<<$executor as DynamicBinaryIterator>::Element>, tuple_positions: TuplePositions, variable_modes: &VariableModes) -> TupleIterator {
                    let checked_tupled = self.filter_map(filter_for_row).map(to_tuple);
                    TupleIterator::$variant(SortedTupleIterator::new(checked_tupled, tuple_positions, variable_modes))
                }
            }
        )*
    };
}

macro_rules! impl_iterator_unbound_inverted {
    ($($executor:ty : $iter:ty => $variant:ident,)*) => {
        $(
            impl ExecutorIteratorUnboundInverted<$executor> for $iter {
                fn unbound_inverted_into_tuple_iterator(self, filter_for_row: Box<FilterMapUnchangedFn<<$executor as DynamicBinaryIterator>::Element>>, to_tuple: MapToTupleFn<<$executor as DynamicBinaryIterator>::Element>, tuple_positions: TuplePositions, variable_modes: &VariableModes) -> TupleIterator {
                    let checked_tupled = self.filter_map(filter_for_row).map(to_tuple);
                    TupleIterator::$variant(SortedTupleIterator::new(checked_tupled, tuple_positions, variable_modes))
                }
            }
        )*
    };
}

macro_rules! impl_iterator_bound_from {
    ($($executor:ty : $iter:ty => $variant:ident,)*) => {
        $(
            impl ExecutorIteratorBoundFrom<$executor> for $iter {
                fn bound_from_into_tuple_iterator(self, filter_for_row: Box<FilterMapUnchangedFn<<$executor as DynamicBinaryIterator>::Element>>, to_tuple: MapToTupleFn<<$executor as DynamicBinaryIterator>::Element>, tuple_positions: TuplePositions, variable_modes: &VariableModes) -> TupleIterator {
                    let checked_tupled = self.filter_map(filter_for_row).map(to_tuple);
                    TupleIterator::$variant(SortedTupleIterator::new(checked_tupled, tuple_positions, variable_modes))
                }
            }
        )*
    };
}

impl_iterator_unbound! {
    HasExecutor: HasIterator => HasSingle,
    HasReverseExecutor : ChainedHasReverseIterator => HasReverseChained,
    IsaExecutor: isa_executor::MultipleTypeIsaIterator => IsaUnbounded,
    IsaReverseExecutor: isa_reverse_executor::MultipleTypeIsaIterator => IsaReverseUnbounded,

    OwnsExecutor: OwnsFlattenedVectorInner => OwnsUnbounded,
    OwnsReverseExecutor: OwnsFlattenedVectorInner => OwnsReverseUnbounded,
    RelatesExecutor: RelatesFlattenedVectorInner => RelatesUnbounded,
    RelatesReverseExecutor: RelatesFlattenedVectorInner => RelatesReverseUnbounded,
    PlaysExecutor: PlaysFlattenedVectorInner => PlaysUnbounded,
    PlaysReverseExecutor: PlaysFlattenedVectorInner => PlaysReverseUnbounded,
    SubExecutor: SubVectorInner => SubUnbounded,
    SubReverseExecutor: SubVectorInner => SubReverseUnbounded,
}

impl_iterator_unbound_inverted! {
    HasExecutor: HasIterator => HasSingle,
    HasExecutor: KMergeBy<HasIterator, HasOrderingFn> => HasMerged,

    HasReverseExecutor : HasReverseIterator => HasReverseSingle,
    HasReverseExecutor : KMergeBy<HasReverseIterator, HasOrderingFn> => HasReverseMerged,
}

impl_iterator_bound_from! {
    HasExecutor: HasIterator => HasSingle,
    HasReverseExecutor : HasReverseIterator => HasReverseSingle,
    IsaExecutor: IsaBoundedInner => IsaBounded,
    IsaReverseExecutor: isa_reverse_executor::MultipleTypeIsaIterator => IsaReverseBounded,
    OwnsExecutor: OwnsVectorInner => OwnsBounded,
    OwnsReverseExecutor: OwnsVectorInner => OwnsReverseBounded,
    RelatesExecutor: RelatesVectorInner => RelatesBounded,
    RelatesReverseExecutor: RelatesVectorInner => RelatesReverseBounded,
    PlaysExecutor: PlaysVectorInner => PlaysBounded,
    PlaysReverseExecutor: PlaysVectorInner => PlaysReverseBounded,
    SubExecutor: SubVectorInner => SubBounded,
    SubReverseExecutor: SubVectorInner => SubReverseBounded,
}

pub(super) struct UnreachableIteratorType<T> {
    phantom: PhantomData<T>,
}
impl<T> Iterator for UnreachableIteratorType<T> {
    type Item = ExecutorResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        unreachable!()
    }
}

impl<T: DynamicBinaryIterator> ExecutorIteratorUnboundInverted<T> for UnreachableIteratorType<T::Element> {
    fn unbound_inverted_into_tuple_iterator(
        self,
        _filter_for_row: Box<FilterMapUnchangedFn<T::Element>>,
        _to_tuple: MapToTupleFn<T::Element>,
        _tuple_positions: TuplePositions,
        _variable_modes: &VariableModes,
    ) -> TupleIterator {
        unreachable!()
    }
}
