/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;
use itertools::KMergeBy;
use compiler::executable::match_::instructions::VariableModes;
use compiler::ExecutorVariable;
use concept::error::ConceptReadError;
use concept::thing::object::{HasIterator, HasReverseIterator};
use ir::pattern::Vertex;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;
use crate::instruction::{DynamicBinaryIterateMode, FilterMapUnchangedFn, isa_executor, isa_reverse_executor, MapToTupleFn, may_get_from_row, TupleSortMode};
use crate::instruction::has_executor::{HasExecutor, HasOrderingFn};
use crate::instruction::has_reverse_executor::{ChainedHasReverseIterator, HasReverseExecutor};
use crate::instruction::isa_executor::{IsaBoundedInner, IsaExecutor};
use crate::instruction::isa_reverse_executor::IsaReverseExecutor;
use crate::instruction::iterator::{SortedTupleIterator, TupleIterator};
use crate::instruction::owns_executor::{OwnsExecutor, OwnsFlattenedVectorInner, OwnsVectorInner};
use crate::instruction::owns_reverse_executor::OwnsReverseExecutor;
use crate::instruction::tuple::TuplePositions;
use crate::pipeline::stage::ExecutionContext;
use crate::row::MaybeOwnedRow;

pub(super) trait DynamicBinaryIterator: Sized {
    type Element;
    fn from(&self) -> &Vertex<ExecutorVariable>;
    fn to(&self) -> &Vertex<ExecutorVariable>;

    fn sort_mode(&self) -> TupleSortMode;

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element>;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element>;

    fn get_iterator_for(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        variable_modes: &VariableModes,
        sort_mode: TupleSortMode,
        row: MaybeOwnedRow<'_>,
        filter_for_row: Box<FilterMapUnchangedFn<Self::Element>>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let tuple_positions = match sort_mode {
            TupleSortMode::From => [self.from().as_variable(), self.to().as_variable()],
            TupleSortMode::To => [self.to().as_variable(), self.from().as_variable()],
        };
        let tuple_positions = TuplePositions::Pair(tuple_positions);

        let dynamic_iterate_mode = DynamicBinaryIterateMode::new(self.from(), self.to(), sort_mode, row.as_reference());
        let from = may_get_from_row(self.from(), &row);
        let to = may_get_from_row(self.to(), &row);

        let iterator = match dynamic_iterate_mode {
            DynamicBinaryIterateMode::UnboundOnFrom => self
                .get_iterator_unbound(context, row)?
                .unbound_into_tuple_iterator(filter_for_row, Self::TUPLE_FROM_TO, tuple_positions, variable_modes),
            DynamicBinaryIterateMode::UnboundOnTo => match self.get_iterator_unbound_inverted(context)? {
                Either::First(single) => single.unbound_inverted_into_tuple_iterator(
                    filter_for_row,
                    Self::TUPLE_TO_FROM,
                    tuple_positions,
                    variable_modes,
                ),
                Either::Second(merged) => merged.unbound_inverted_into_tuple_iterator(
                    filter_for_row,
                    Self::TUPLE_TO_FROM,
                    tuple_positions,
                    variable_modes,
                ),
            },
            DynamicBinaryIterateMode::BoundFromOnTo => {
                // TODO: We ensure the function does the mapping. But we need to undo it for BoundFromSwapped anyway?
                // So this function should take charge of the direction and allow the delegates to return their standard direction
                self.get_iterator_bound_from(context, row.as_reference())?.bound_from_into_tuple_iterator(
                    filter_for_row,
                    Self::TUPLE_TO_FROM,
                    tuple_positions,
                    variable_modes,
                )
            }
            // DynamicBinaryIterateMode::BoundFromSwapped => {}
            // DynamicBinaryIterateMode::BoundToUsingReverse => {}
            // DynamicBinaryIterateMode::BoundToUsingReverseSwapped => {}
            DynamicBinaryIterateMode::CheckOnFrom => {
                debug_assert!(from.is_some() && to.is_some());
                let optional_element = self.get_iterator_check(context, row)?;
                let optional_tuple_result =
                    optional_element.map(|x| Ok(x)).and_then(filter_for_row).map(Self::TUPLE_FROM_TO);
                TupleIterator::Check(SortedTupleIterator::new(
                    optional_tuple_result.into_iter(),
                    tuple_positions,
                    variable_modes,
                ))
            }
            // DynamicBinaryIterateMode::CheckOnTo => {}
            _ => {
                todo!("Hit {:?} for {:?}", dynamic_iterate_mode, std::any::type_name::<Self>())
            }
        };
        Ok(iterator)
    }

    fn get_iterator_unbound(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>>;
    fn get_iterator_unbound_inverted(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<
        Either<impl ExecutorIteratorUnboundInverted<Self>, impl ExecutorIteratorUnboundInverted<Self>>,
        Box<ConceptReadError>,
    >;

    fn get_iterator_bound_from(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>>;

    fn get_iterator_check(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<Option<Self::Element>, Box<ConceptReadError>>;
}
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
