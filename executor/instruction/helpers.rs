/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use compiler::{executable::match_::instructions::VariableModes, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    thing::object::{HasIterator, HasReverseIterator},
};
use ir::pattern::Vertex;
use itertools::KMergeBy;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        has_executor::{HasExecutor, HasOrderingFn},
        has_reverse_executor::{ChainedHasReverseIterator, HasReverseExecutor},
        isa_executor,
        isa_executor::{IsaBoundedInner, IsaExecutor},
        isa_reverse_executor,
        isa_reverse_executor::IsaReverseExecutor,
        iterator::{SortedTupleIterator, TupleIterator},
        may_get_from_row,
        owns_executor::{OwnsExecutor, OwnsFlattenedVectorInner, OwnsVectorInner},
        owns_reverse_executor::OwnsReverseExecutor,
        plays_executor::{PlaysExecutor, PlaysFilterMapFn, PlaysFlattenedVectorInner, PlaysVectorInner},
        plays_reverse_executor::PlaysReverseExecutor,
        relates_executor::{RelatesExecutor, RelatesFlattenedVectorInner, RelatesVectorInner},
        relates_reverse_executor::RelatesReverseExecutor,
        sub_executor::{SubExecutor, SubVectorInner},
        sub_reverse_executor::SubReverseExecutor,
        tuple::TuplePositions,
        BinaryTupleSortMode, Checker, DynamicBinaryIterateMode, FilterFn, FilterMapUnchangedFn, MapToTupleFn,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(super) trait DynamicBinaryIterator: Sized {
    type Element;
    fn from(&self) -> &Vertex<ExecutorVariable>;
    fn to(&self) -> &Vertex<ExecutorVariable>;

    fn sort_mode(&self) -> BinaryTupleSortMode;

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element>;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element>;

    // Methods to implement
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

    fn filter_fn_unbound(&self) -> Option<Arc<FilterFn<Self::Element>>>;

    // Is also unbound inverted.
    fn filter_fn_bound_from(&self) -> Option<Arc<FilterFn<Self::Element>>>;
    fn filter_fn_bound_to(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        todo!("Might need to merge the Forward & Reverse Executors together")
    }

    fn create_filter_for_row(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
        filter_fn_opt: Option<Arc<FilterFn<Self::Element>>>,
        checker: &Checker<Self::Element>,
    ) -> Box<FilterMapUnchangedFn<Self::Element>> {
        let check = checker.filter_for_row(context, &row);
        if let Some(filter_fn) = filter_fn_opt {
            let filter = filter_fn.clone();
            Box::new(move |item| match filter(&item) {
                Ok(true) => match check(&item) {
                    Ok(true) | Err(_) => Some(item),
                    Ok(false) => None,
                },
                Ok(false) => None,
                Err(_) => Some(item),
            })
        } else {
            Box::new(move |item| match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            })
        }
    }

    // Common method to handle the dynamic mode logic.
    fn get_iterator_for(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        variable_modes: &VariableModes,
        sort_mode: BinaryTupleSortMode,
        tuple_positions: TuplePositions,
        row: MaybeOwnedRow<'_>,
        checker: &Checker<Self::Element>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let dynamic_iterate_mode = DynamicBinaryIterateMode::new(self.from(), self.to(), sort_mode, row.as_reference());
        let from = may_get_from_row(self.from(), &row);
        let to = may_get_from_row(self.to(), &row);

        let iterator = match dynamic_iterate_mode {
            DynamicBinaryIterateMode::UnboundOnFrom => {
                let filter_for_row =
                    self.create_filter_for_row(context, row.as_reference(), self.filter_fn_unbound(), checker);
                self.get_iterator_unbound(context, row)?.unbound_into_tuple_iterator(
                    filter_for_row,
                    Self::TUPLE_FROM_TO,
                    tuple_positions,
                    variable_modes,
                )
            }
            DynamicBinaryIterateMode::UnboundOnTo => {
                let filter_for_row =
                    self.create_filter_for_row(context, row.as_reference(), self.filter_fn_bound_from(), checker);
                match self.get_iterator_unbound_inverted(context)? {
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
                }
            }
            DynamicBinaryIterateMode::BoundFromOnFrom => {
                let filter_for_row =
                    self.create_filter_for_row(context, row.as_reference(), self.filter_fn_bound_from(), checker);
                self.get_iterator_bound_from(context, row.as_reference())?.bound_from_into_tuple_iterator(
                    filter_for_row,
                    Self::TUPLE_FROM_TO,
                    tuple_positions,
                    variable_modes,
                )
            }
            DynamicBinaryIterateMode::BoundFromOnTo => {
                let filter_for_row =
                    self.create_filter_for_row(context, row.as_reference(), self.filter_fn_bound_from(), checker);
                self.get_iterator_bound_from(context, row.as_reference())?.bound_from_into_tuple_iterator(
                    filter_for_row,
                    Self::TUPLE_TO_FROM,
                    tuple_positions,
                    variable_modes,
                )
            }

            DynamicBinaryIterateMode::BoundToOnFromUsingReverse => {
                todo!("Might need to merge the Forward & Reverse Executors together")
            }
            DynamicBinaryIterateMode::BoundToOnToUsingReverse => {
                todo!("Might need to merge the Forward & Reverse Executors together")
            }
            DynamicBinaryIterateMode::CheckOnFrom => {
                let filter_for_row =
                    self.create_filter_for_row(context, row.as_reference(), self.filter_fn_bound_from(), checker);
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
            DynamicBinaryIterateMode::CheckOnTo => {
                let filter_for_row =
                    self.create_filter_for_row(context, row.as_reference(), self.filter_fn_bound_from(), checker);
                debug_assert!(from.is_some() && to.is_some());
                let optional_element = self.get_iterator_check(context, row)?;
                let optional_tuple_result =
                    optional_element.map(|x| Ok(x)).and_then(filter_for_row).map(Self::TUPLE_TO_FROM);
                TupleIterator::Check(SortedTupleIterator::new(
                    optional_tuple_result.into_iter(),
                    tuple_positions,
                    variable_modes,
                ))
            }
        };
        Ok(iterator)
    }
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
