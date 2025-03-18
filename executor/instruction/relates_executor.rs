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

use answer::{variable_value::VariableValue, Type};
use compiler::{executable::match_::instructions::type_::RelatesInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{relation_type::RelationType, role_type::RoleType, type_manager::TypeManager},
};
use itertools::Itertools;
use ir::pattern::Vertex;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        relates_reverse_executor::RelatesReverseExecutor,
        tuple::{relates_to_tuple_relation_role, relates_to_tuple_role_relation, RelatesToTupleFn, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, FilterMapUnchangedFn, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};
use crate::instruction::helpers::{DynamicBinaryIterator, ExecutorIteratorBoundFrom, ExecutorIteratorUnbound, ExecutorIteratorUnboundInverted, UnreachableIteratorType};
use crate::instruction::{MapToTupleFn, BinaryTupleSortMode, sort_mode_and_tuple_positions};

pub(crate) struct RelatesExecutor {
    relates: ir::pattern::constraint::Relates<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    relation_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
    role_types: Arc<BTreeSet<Type>>,
    filter_fn_unbound: Arc<RelatesFilterFn>, filter_fn_bound: Arc<RelatesFilterFn>,
    checker: Checker<(RelationType, RoleType)>,
    sort_mode: BinaryTupleSortMode,
}

impl fmt::Debug for RelatesExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelatesExecutor")
    }
}

pub(super) type RelatesTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<RelatesFilterMapFn>>, RelatesToTupleFn>;

pub(super) type RelatesFlattenedVectorInner = iter::Map<
    iter::Flatten<vec::IntoIter<BTreeSet<(RelationType, RoleType)>>>,
    fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
>;
pub(super) type RelatesVectorInner = iter::Map<
    vec::IntoIter<(RelationType, RoleType)>,
    fn((RelationType, RoleType)) -> Result<(RelationType, RoleType), Box<ConceptReadError>>,
>;

pub(super) type RelatesUnboundedSortedRelation = RelatesTupleIterator<RelatesFlattenedVectorInner>;
pub(super) type RelatesBoundedSortedRole = RelatesTupleIterator<RelatesVectorInner>;

pub(super) type RelatesFilterFn = FilterFn<(RelationType, RoleType)>;
pub(super) type RelatesFilterMapFn = FilterMapUnchangedFn<(RelationType, RoleType)>;

pub(super) type RelatesVariableValueExtractor = for<'a> fn(&'a (RelationType, RoleType)) -> VariableValue<'a>;
pub(super) const EXTRACT_RELATION: RelatesVariableValueExtractor =
    |(relation, _)| VariableValue::Type(Type::Relation(*relation));
pub(super) const EXTRACT_ROLE: RelatesVariableValueExtractor = |(_, role)| VariableValue::Type(Type::RoleType(*role));

impl RelatesExecutor {
    pub(crate) fn new(
        relates: RelatesInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let role_types = relates.role_types().clone();
        let relation_role_types = relates.relation_role_types().clone();
        debug_assert!(role_types.len() > 0);

        let RelatesInstruction { relates, checks, .. } = relates;

        let iterate_mode = BinaryIterateMode::new(relates.relation(), relates.role_type(), &variable_modes, sort_by);
        let (sort_mode, output_tuple_positions) = sort_mode_and_tuple_positions(relates.relation(), relates.role_type(), sort_by);
        let filter_fn_unbound = create_relates_filter_relation_role_type(relation_role_types.clone());
        let filter_fn_bound = create_relates_filter_role_type(role_types.clone());

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
            relation_role_types,
            role_types,
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

    fn get_relates_for_relation(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        relation: Type,
    ) -> Result<BTreeSet<(RelationType, RoleType)>, Box<ConceptReadError>> {
        let relation_type = relation.as_relation_type();

        Ok(relation_type
            .get_related_role_types(snapshot, type_manager)?
            .into_iter()
            .map(|role_type| (relation_type, role_type))
            .collect())
    }
}

impl fmt::Display for RelatesExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.relates, &self.iterate_mode)
    }
}

fn create_relates_filter_relation_role_type(
    relation_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((relation, role)) => match relation_role_types.get(&Type::from(*relation)) {
            Some(role_types) => Ok(role_types.contains(&Type::RoleType(*role))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_relates_filter_role_type(role_types: Arc<BTreeSet<Type>>) -> Arc<RelatesFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, role)) => Ok(role_types.contains(&Type::RoleType(*role))),
        Err(err) => Err(err.clone()),
    })
}

impl DynamicBinaryIterator for RelatesExecutor {
    type Element = (RelationType, RoleType);

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.relates.relation()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.relates.role_type()
    }

    fn sort_mode(&self) -> BinaryTupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = relates_to_tuple_relation_role;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = relates_to_tuple_role_relation;
    fn get_iterator_unbound(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>> {
        let type_manager = context.type_manager();
        let relates: Vec<_> = self
            .relation_role_types
            .keys()
            .map(|relation| self.get_relates_for_relation(&*context.snapshot, type_manager, *relation))
            .try_collect()?;
        let iterator = relates.into_iter().flatten().map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_unbound_inverted(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>) -> Result<Either<UnreachableIteratorType<Self::Element>, UnreachableIteratorType<Self::Element>>, Box<ConceptReadError>> {
        // is this ever relevant?
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
        }));
    }

    fn get_iterator_bound_from(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>> {
        let relation =
            type_from_row_or_annotations(self.relates.relation(), row, self.relation_role_types.keys());
        let type_manager = context.type_manager();
        let relates = self.get_relates_for_relation(&*context.snapshot, type_manager, relation)?;

        let iterator =
            relates.iter().cloned().sorted_by_key(|(relation, role)| (*role, *relation)).map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_check(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let relation = type_from_row_or_annotations(self.from(), row.as_reference(), self.relation_role_types.keys());
        let role = type_from_row_or_annotations(self.to(), row, self.role_types.iter());
        Ok(self.relation_role_types.get(&relation).unwrap().contains(&role)
            .then(|| (relation.as_relation_type(), role.as_role_type())))
    }

    fn filter_fn_unbound(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_unbound.clone())
    }

    fn filter_fn_bound(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_bound.clone())
    }
}