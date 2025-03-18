/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    vec,
};

use answer::Type;
use compiler::{executable::match_::instructions::type_::SubReverseInstruction, ExecutorVariable};
use concept::error::ConceptReadError;
use error::UnimplementedFeature;
use ir::pattern::Vertex;
use itertools::Itertools;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        helpers::{
            ExecutorIteratorBoundFrom, ExecutorIteratorUnbound, ExecutorIteratorUnboundInverted,
            UnreachableIteratorType,
        },
        iterator::{SortedTupleIterator, TupleIterator},
        sort_mode_and_tuple_positions,
        sub_executor::{SubExecutor, SubFilterFn, SubFilterMapFn, SubTupleIterator, EXTRACT_SUB, EXTRACT_SUPER},
        tuple::{sub_to_tuple_sub_super, sub_to_tuple_super_sub, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, BinaryTupleSortMode, Checker, DynamicBinaryIterator, FilterFn,
        MapToTupleFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

pub(crate) struct SubReverseExecutor {
    sub: ir::pattern::constraint::Sub<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    super_to_subtypes: Arc<BTreeMap<Type, Vec<Type>>>,
    subtypes: Arc<BTreeSet<Type>>,
    filter_fn_unbound: Arc<SubFilterFn>,
    filter_fn_bound: Arc<SubFilterFn>,
    checker: Checker<(Type, Type)>,
    sort_mode: BinaryTupleSortMode,
}

impl fmt::Debug for SubReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SubReverseExecutor")
    }
}

pub(super) type SubReverseUnboundedSortedSuper =
    SubTupleIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>;
pub(super) type SubReverseBoundedSortedSub =
    SubTupleIterator<vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>>;

impl SubReverseExecutor {
    pub(crate) fn new(
        sub: SubReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let subtypes = sub.subtypes().clone();
        let super_to_subtypes = sub.super_to_subtypes().clone();
        debug_assert!(subtypes.len() > 0);

        let SubReverseInstruction { sub, checks, .. } = sub;

        let iterate_mode = BinaryIterateMode::new(sub.supertype(), sub.subtype(), &variable_modes, sort_by);
        let (sort_mode, output_tuple_positions) =
            sort_mode_and_tuple_positions(sub.supertype(), sub.subtype(), sort_by);
        let filter_fn_unbound = create_sub_filter_super_sub(super_to_subtypes.clone());
        let filter_fn_bound = create_sub_filter_sub(subtypes.clone());

        let subtype = sub.subtype().as_variable();
        let supertype = sub.supertype().as_variable();
        let checker = Checker::<(Type, Type)>::new(
            checks,
            [(subtype, EXTRACT_SUB), (supertype, EXTRACT_SUPER)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            sub,
            sort_mode,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            super_to_subtypes,
            subtypes,
            filter_fn_unbound,
            filter_fn_bound,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        self.get_iterator_for(
            context,
            &self.variable_modes,
            self.sort_mode,
            self.tuple_positions.clone(),
            row,
            &self.checker,
        )
    }
}

impl fmt::Display for SubReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.sub, &self.iterate_mode)
    }
}

fn create_sub_filter_super_sub(super_to_subtypes: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, sup)) => match super_to_subtypes.get(sup) {
            Some(subtypes) => Ok(subtypes.contains(sub)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_sub(subtypes: Arc<BTreeSet<Type>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, _)) => Ok(subtypes.contains(sub)),
        Err(err) => Err(err.clone()),
    })
}

impl DynamicBinaryIterator for SubReverseExecutor {
    type Element = (Type, Type);

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.sub.supertype()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.sub.subtype()
    }

    fn sort_mode(&self) -> BinaryTupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = SubExecutor::TUPLE_TO_FROM;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = SubExecutor::TUPLE_FROM_TO;

    fn get_iterator_unbound(
        &self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        _row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>> {
        let sub_with_super =
            self.super_to_subtypes.iter().flat_map(|(sup, subs)| subs.iter().map(|sub| Ok((*sub, *sup)))).collect_vec();
        Ok(sub_with_super.into_iter())
    }

    fn get_iterator_unbound_inverted(
        &self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<
        Either<UnreachableIteratorType<Self::Element>, UnreachableIteratorType<Self::Element>>,
        Box<ConceptReadError>,
    > {
        // TODO: Is this ever relevant?
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
        }));
    }

    fn get_iterator_bound_from(
        &self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>> {
        let supertype = type_from_row_or_annotations(self.sub.supertype(), row, self.super_to_subtypes.keys());
        let subtypes = self.super_to_subtypes.get(&supertype).unwrap_or(const { &Vec::new() });
        let sub_with_super = subtypes.iter().map(|sub| Ok((*sub, supertype))).collect_vec(); // TODO cache this
        Ok(sub_with_super.into_iter())
    }

    fn get_iterator_check(
        &self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let supertype = type_from_row_or_annotations(self.from(), row.as_reference(), self.super_to_subtypes.keys());
        let subtype = type_from_row_or_annotations(self.to(), row, self.subtypes.iter());
        Ok(self.super_to_subtypes.get(&supertype).unwrap().contains(&subtype).then(|| (subtype, supertype)))
    }

    fn filter_fn_unbound(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_unbound.clone())
    }

    fn filter_fn_bound_from(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_bound.clone())
    }
}
