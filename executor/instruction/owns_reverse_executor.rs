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
use compiler::{executable::match_::instructions::type_::OwnsReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{attribute_type::AttributeType, object_type::ObjectType, OwnerAPI},
};
use ir::pattern::Vertex;
use itertools::Itertools;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        helpers::{DynamicBinaryIterator,
                  ExecutorIteratorBoundFrom, ExecutorIteratorUnbound, ExecutorIteratorUnboundInverted, UnreachableIteratorType},
        iterator::{SortedTupleIterator, TupleIterator},
        owns_executor::{
            OwnsExecutor, OwnsFilterFn, OwnsFilterMapFn, OwnsFlattenedVectorInner, OwnsTupleIterator,
            OwnsVariableValueExtractor, OwnsVectorInner, EXTRACT_ATTRIBUTE, EXTRACT_OWNER,
        },
        plays_executor::PlaysExecutor,
        tuple::{owns_to_tuple_attribute_owner, owns_to_tuple_owner_attribute, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, DynamicBinaryIterateMode, MapToTupleFn,
        BinaryTupleSortMode, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};
use crate::instruction::{FilterFn, sort_mode_and_tuple_positions};

pub(crate) struct OwnsReverseExecutor {
    owns: ir::pattern::constraint::Owns<ExecutorVariable>,
    sort_mode: BinaryTupleSortMode,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<BTreeSet<Type>>,
    filter_fn_unbound: Arc<OwnsFilterFn>, filter_fn_bound: Arc<OwnsFilterFn>,
    checker: Checker<(ObjectType, AttributeType)>,
}

impl fmt::Debug for OwnsReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OwnsReverseExecutor")
    }
}

pub(super) type OwnsReverseUnboundedSortedAttribute = OwnsTupleIterator<
    iter::Map<
        iter::Flatten<vec::IntoIter<BTreeSet<(ObjectType, AttributeType)>>>,
        fn((ObjectType, AttributeType)) -> Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
    >,
>;
pub(super) type OwnsReverseBoundedSortedOwner = OwnsTupleIterator<
    iter::Map<
        vec::IntoIter<(ObjectType, AttributeType)>,
        fn((ObjectType, AttributeType)) -> Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
    >,
>;

impl OwnsReverseExecutor {
    pub(crate) fn new(
        owns: OwnsReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let owner_types = owns.owner_types().clone();
        let attribute_owner_types = owns.attribute_owner_types().clone();
        debug_assert!(owner_types.len() > 0);

        let OwnsReverseInstruction { owns, checks, .. } = owns;

        let iterate_mode = BinaryIterateMode::new(owns.attribute(), owns.owner(), &variable_modes, sort_by);
        let (sort_mode, output_tuple_positions) = sort_mode_and_tuple_positions(owns.attribute(), owns.owner(), sort_by);
        let filter_fn_unbound = create_owns_filter_owner_attribute(attribute_owner_types.clone());
        let filter_fn_bound =create_owns_filter_attribute(owner_types.clone());

        let owner = owns.owner().as_variable();
        let attribute = owns.attribute().as_variable();
        let checker = Checker::<(ObjectType, AttributeType)>::new(
            checks,
            [(owner, EXTRACT_OWNER), (attribute, EXTRACT_ATTRIBUTE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect::<HashMap<ExecutorVariable, OwnsVariableValueExtractor>>(),
        );

        Self {
            owns,
            sort_mode,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            attribute_owner_types,
            owner_types,
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

impl fmt::Display for OwnsReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.owns, &self.iterate_mode)
    }
}

fn create_owns_filter_owner_attribute(attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((owner, attribute)) => match attribute_owner_types.get(&Type::Attribute(*attribute)) {
            Some(owner_types) => Ok(owner_types.contains(&Type::from(*owner))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_owns_filter_attribute(owner_types: Arc<BTreeSet<Type>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((owner, _)) => Ok(owner_types.contains(&Type::from(*owner))),
        Err(err) => Err(err.clone()),
    })
}

impl DynamicBinaryIterator for OwnsReverseExecutor {
    type Element = (ObjectType, AttributeType);

    fn from(&self) -> &Vertex<ExecutorVariable> {
        todo!()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        todo!()
    }

    fn sort_mode(&self) -> BinaryTupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = OwnsExecutor::TUPLE_TO_FROM;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = OwnsExecutor::TUPLE_FROM_TO;

    fn get_iterator_unbound(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        _row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>> {
        let type_manager = context.type_manager();
        let owns: Vec<_> = self
            .attribute_owner_types
            .keys()
            .map(|attribute| {
                let attribute_type = attribute.as_attribute_type();
                attribute_type
                    .get_owner_types(&*context.snapshot, type_manager)
                    .map(|res| res.to_owned().keys().map(|object_type| (*object_type, attribute_type)).collect())
            })
            .try_collect()?;
        let iterator: OwnsFlattenedVectorInner = owns.into_iter().flatten().map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_unbound_inverted(
        &self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<
        Either<UnreachableIteratorType<Self::Element>, UnreachableIteratorType<Self::Element>>,
        Box<ConceptReadError>,
    > {
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
        }));
    }

    fn get_iterator_bound_from(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>> {
        let attribute_type =
            type_from_row_or_annotations(self.owns.attribute(), row, self.attribute_owner_types.keys())
                .as_attribute_type();
        let type_manager = context.type_manager();
        let owns = attribute_type
            .get_owner_types(&*context.snapshot, type_manager)?
            .to_owned()
            .into_keys()
            .map(|object_type| (object_type, attribute_type));

        let iterator = owns.sorted_by_key(|(owner, _)| *owner).map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_check(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let attribute =
            type_from_row_or_annotations(self.from(), row.as_reference(), self.attribute_owner_types.keys());
        let owner = type_from_row_or_annotations(self.to(), row, self.owner_types.iter());
        Ok(self.attribute_owner_types.get(&attribute).unwrap().contains(&owner)
            .then(|| (owner.as_object_type(), attribute.as_attribute_type())))
    }


    fn filter_fn_unbound(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_unbound.clone())
    }

    fn filter_fn_bound(&self) -> Option<Arc<FilterFn<Self::Element>>> {
        Some(self.filter_fn_bound.clone())
    }
}
