/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt, iter,
    sync::Arc,
    vec,
};

use answer::Type;
use compiler::{executable::match_::instructions::type_::RelatesReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{relation_type::RelationType, role_type::RoleType},
};
use itertools::Itertools;
use ir::pattern::Vertex;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        relates_executor::{
            RelatesFilterFn, RelatesFilterMapFn, RelatesTupleIterator, RelatesVariableValueExtractor, EXTRACT_RELATION,
            EXTRACT_ROLE,
        },
        tuple::{relates_to_tuple_relation_role, relates_to_tuple_role_relation, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};
use crate::instruction::helpers::{DynamicBinaryIterator, ExecutorIteratorBoundFrom, ExecutorIteratorUnbound, ExecutorIteratorUnboundInverted, UnreachableIteratorType};
use crate::instruction::{MapToTupleFn, BinaryTupleSortMode, sort_mode_and_tuple_positions, FilterFn};
use crate::instruction::relates_executor::RelatesExecutor;

pub(crate) struct RelatesReverseExecutor {
    relates: ir::pattern::constraint::Relates<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_types: Arc<BTreeSet<Type>>,
    filter_fn_unbound: Arc<RelatesFilterFn>, filter_fn_bound: Arc<RelatesFilterFn>,
    checker: Checker<(RelationType, RoleType)>,
    sort_mode: BinaryTupleSortMode,
}

impl fmt::Debug for RelatesReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelatesReverseExecutor")
    }
}

pub(super) type RelatesReverseUnboundedSortedRole = RelatesTupleIterator<
    iter::Map<
        iter::Flatten<vec::IntoIter<BTreeSet<(RelationType, RoleType)>>>,
        fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
    >,
>;
pub(super) type RelatesReverseBoundedSortedRelation = RelatesTupleIterator<
    iter::Map<
        vec::IntoIter<(RelationType, RoleType)>,
        fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
    >,
>;

impl RelatesReverseExecutor {
    pub(crate) fn new(
        relates: RelatesReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let relation_types = relates.relation_types().clone();
        let role_relation_types = relates.role_relation_types().clone();
        debug_assert!(relation_types.len() > 0);

        let RelatesReverseInstruction { relates, checks, .. } = relates;

        let iterate_mode = BinaryIterateMode::new(relates.role_type(), relates.relation(), &variable_modes, sort_by);
        let (sort_mode, output_tuple_positions) = sort_mode_and_tuple_positions(relates.role_type(), relates.relation(), sort_by);
        let filter_fn_unbound = create_relates_filter_relation_role(role_relation_types.clone());
        let filter_fn_bound = create_relates_filter_role(relation_types.clone());

        let relation = relates.relation().as_variable();
        let role_type = relates.role_type().as_variable();
        let checker = Checker::<(RelationType, RoleType)>::new(
            checks,
            [(relation, EXTRACT_RELATION), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect::<HashMap<ExecutorVariable, RelatesVariableValueExtractor>>(),
        );

        Self {
            relates,
            sort_mode,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            role_relation_types,
            relation_types,
            filter_fn_unbound, filter_fn_bound,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        self.get_iterator_for(context, &self.variable_modes, self.sort_mode, self.tuple_positions.clone(), row, &self.checker)
    }
}

impl fmt::Display for RelatesReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.relates, &self.iterate_mode)
    }
}

fn create_relates_filter_relation_role(role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((relation, role)) => match role_relation_types.get(&Type::RoleType(*role)) {
            Some(relation_types) => Ok(relation_types.contains(&Type::from(*relation))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_relates_filter_role(relation_types: Arc<BTreeSet<Type>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((relation, _)) => Ok(relation_types.contains(&Type::from(*relation))),
        Err(err) => Err(err.clone()),
    })
}

impl DynamicBinaryIterator for RelatesReverseExecutor {
    type Element = (RelationType, RoleType);

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.relates.role_type()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.relates.relation()
    }

    fn sort_mode(&self) -> BinaryTupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = RelatesExecutor::TUPLE_TO_FROM;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = RelatesExecutor::TUPLE_FROM_TO;

    fn get_iterator_unbound(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>> {
        let type_manager = context.type_manager();
        let relates: Vec<_> = self
            .role_relation_types
            .keys()
            .map(|role| {
                let role_type = role.as_role_type();
                role_type
                    .get_relation_types(&*context.snapshot, type_manager)
                    .map(|res| res.to_owned().keys().map(|relation_type| (*relation_type, role_type)).collect())
            })
            .try_collect()?;
        let iterator = relates.into_iter().flatten().map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_unbound_inverted(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>) -> Result<Either<UnreachableIteratorType<Self::Element>, UnreachableIteratorType<Self::Element>>, Box<ConceptReadError>> {
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
        }));
    }

    fn get_iterator_bound_from(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>> {
        let role_type =
            type_from_row_or_annotations(self.relates.role_type(), row, self.role_relation_types.keys())
                .as_role_type();
        let relates = role_type
            .get_relation_types(&*context.snapshot, context.type_manager())?
            .to_owned()
            .into_keys()
            .map(|relation_type| (relation_type, role_type));

        let iterator = relates.into_iter().sorted_by_key(|(relation, _)| *relation).map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_check(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let role =
            type_from_row_or_annotations(self.from(), row.as_reference(), self.role_relation_types.keys());
        let relation = type_from_row_or_annotations(self.to(), row, self.relation_types.iter());
        Ok(self.role_relation_types.get(&role).unwrap().contains(&relation)
            .then(|| (relation.as_relation_type(), role.as_role_type())))
    }

    fn filter_fn_unbound(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_unbound.clone())
    }

    fn filter_fn_bound(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_bound.clone())
    }
}