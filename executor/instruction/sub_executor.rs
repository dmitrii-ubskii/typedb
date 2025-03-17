/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, iter,
    sync::Arc,
    vec,
};

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::SubInstruction, ExecutorVariable};
use concept::error::ConceptReadError;
use itertools::Itertools;
use ir::pattern::Vertex;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{sub_to_tuple_sub_super, sub_to_tuple_super_sub, SubToTupleFn, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};
use crate::instruction::helpers::{DynamicBinaryIterator, ExecutorIteratorBoundFrom, ExecutorIteratorUnbound, ExecutorIteratorUnboundInverted, UnreachableIteratorType};
use crate::instruction::{MapToTupleFn, BinaryTupleSortMode, sort_mode_and_tuple_positions};

pub(crate) struct SubExecutor {
    sub: ir::pattern::constraint::Sub<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>,
    supertypes: Arc<BTreeSet<Type>>,
    filter_fn: Arc<SubFilterFn>,
    checker: Checker<(Type, Type)>,
    sort_mode: BinaryTupleSortMode,
}

impl fmt::Debug for SubExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SubExecutor")
    }
}

pub(super) type SubVectorInner = vec::IntoIter<Result<(Type, Type), Box<ConceptReadError>>>;
pub(super) type SubTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<SubFilterMapFn>>, SubToTupleFn>;
pub(super) type SubUnboundedSortedSub = SubTupleIterator<SubVectorInner>;
pub(super) type SubBoundedSortedSuper = SubTupleIterator<SubVectorInner>;

pub(super) type SubFilterFn = FilterFn<(Type, Type)>;
pub(super) type SubFilterMapFn = FilterMapUnchangedFn<(Type, Type)>;

type SubVariableValueExtractor = fn(&(Type, Type)) -> VariableValue<'_>;
pub(super) const EXTRACT_SUB: SubVariableValueExtractor = |(sub, _)| VariableValue::Type(*sub);
pub(super) const EXTRACT_SUPER: SubVariableValueExtractor = |(_, sup)| VariableValue::Type(*sup);

impl SubExecutor {
    pub(crate) fn new(
        sub: SubInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let supertypes = sub.supertypes().clone();
        let sub_to_supertypes = sub.sub_to_supertypes().clone();
        debug_assert!(supertypes.len() > 0);

        let SubInstruction { sub, checks, .. } = sub;

        let iterate_mode = BinaryIterateMode::new(sub.subtype(), sub.supertype(), &variable_modes, sort_by);
        let (sort_mode, output_tuple_positions) = sort_mode_and_tuple_positions(sub.subtype(), sub.supertype(), sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_sub_filter_sub_super(sub_to_supertypes.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_sub_filter_super(supertypes.clone())
            }
        };

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
            sub_to_supertypes,
            supertypes,
            filter_fn,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let filter = self.filter_fn.clone();
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<SubFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });
        self.get_iterator_for(context, &self.variable_modes, self.sort_mode, self.tuple_positions.clone(), row, filter_for_row)
    }
}

impl fmt::Display for SubExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.sub, &self.iterate_mode)
    }
}

fn create_sub_filter_sub_super(sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((sub, sup)) => match sub_to_supertypes.get(sub) {
            Some(supertypes) => Ok(supertypes.contains(sup)),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_sub_filter_super(supertypes: Arc<BTreeSet<Type>>) -> Arc<SubFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, sup)) => Ok(supertypes.contains(sup)),
        Err(err) => Err(err.clone()),
    })
}

impl DynamicBinaryIterator for SubExecutor {
    type Element = (Type, Type);

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.sub.subtype()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.sub.supertype()
    }

    fn sort_mode(&self) -> BinaryTupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = sub_to_tuple_sub_super;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = sub_to_tuple_super_sub;

    fn get_iterator_unbound(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>> {
        let sub_with_super = self
            .sub_to_supertypes
            .iter()
            .flat_map(|(sub, supers)| supers.iter().map(|sup| Ok((*sub, *sup))))
            .collect_vec();
        Ok(sub_with_super.into_iter())
    }

    fn get_iterator_unbound_inverted(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>) -> Result<Either<UnreachableIteratorType<Self::Element>, UnreachableIteratorType<Self::Element>>, Box<ConceptReadError>> {
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
        }));
    }

    fn get_iterator_bound_from(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>> {
        let subtype = type_from_row_or_annotations(self.sub.subtype(), row, self.sub_to_supertypes.keys());
        let supertypes = self.sub_to_supertypes.get(&subtype).unwrap_or(const { &Vec::new() });
        let sub_with_super = supertypes.iter().map(|sup| Ok((subtype, *sup))).collect_vec(); // TODO cache this
        Ok(sub_with_super.into_iter())
    }

    fn get_iterator_check(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let subtype = type_from_row_or_annotations(self.from(), row.as_reference(), self.sub_to_supertypes.keys());
        let supertype = type_from_row_or_annotations(self.to(), row, self.supertypes.iter());
        Ok(self.sub_to_supertypes.get(&subtype).unwrap().contains(&supertype)
            .then(|| (subtype, supertype)))
    }
}