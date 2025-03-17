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
use compiler::{executable::match_::instructions::type_::OwnsInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{
        attribute_type::AttributeType, object_type::ObjectType, type_manager::TypeManager, ObjectTypeAPI, OwnerAPI,
    },
};
use error::UnimplementedFeature;
use ir::pattern::Vertex;
use itertools::Itertools;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        helpers::{ DynamicBinaryIterator, ExecutorIteratorBoundFrom,
                   ExecutorIteratorUnbound, ExecutorIteratorUnboundInverted, UnreachableIteratorType},
        iterator::{SortedTupleIterator, TupleIterator},
        owns_reverse_executor::OwnsReverseExecutor,
        tuple::{owns_to_tuple_attribute_owner, owns_to_tuple_owner_attribute, OwnsToTupleFn, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, FilterFn, FilterMapUnchangedFn, MapToTupleFn,
        BinaryTupleSortMode, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};
use crate::instruction::sort_mode_and_tuple_positions;

pub(crate) struct OwnsExecutor {
    owns: ir::pattern::constraint::Owns<ExecutorVariable>,
    sort_mode: BinaryTupleSortMode,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<OwnsFilterFn>,
    checker: Checker<(ObjectType, AttributeType)>,
}

impl fmt::Debug for OwnsExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OwnsExecutor")
    }
}

pub(super) type OwnsTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<OwnsFilterMapFn>>, OwnsToTupleFn>;

pub(super) type OwnsFlattenedVectorInner = iter::Map<
    iter::Flatten<vec::IntoIter<BTreeSet<(ObjectType, AttributeType)>>>,
    fn((ObjectType, AttributeType)) -> Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
>;
pub(super) type OwnsVectorInner = iter::Map<
    vec::IntoIter<(ObjectType, AttributeType)>,
    fn((ObjectType, AttributeType)) -> Result<(ObjectType, AttributeType), Box<ConceptReadError>>,
>;

pub(super) type OwnsUnboundedSortedOwner = OwnsTupleIterator<OwnsFlattenedVectorInner>;
pub(super) type OwnsBoundedSortedAttribute = OwnsTupleIterator<OwnsVectorInner>;

pub(super) type OwnsFilterFn = FilterFn<(ObjectType, AttributeType)>;
pub(super) type OwnsFilterMapFn = FilterMapUnchangedFn<(ObjectType, AttributeType)>;

pub(super) type OwnsVariableValueExtractor = for<'a> fn(&'a (ObjectType, AttributeType)) -> VariableValue<'a>;
pub(super) const EXTRACT_OWNER: OwnsVariableValueExtractor = |(owner, _)| VariableValue::Type(Type::from(*owner));
pub(super) const EXTRACT_ATTRIBUTE: OwnsVariableValueExtractor =
    |(_, attribute)| VariableValue::Type(Type::Attribute(*attribute));

impl OwnsExecutor {
    pub(crate) fn new(
        owns: OwnsInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let attribute_types = owns.attribute_types().clone();
        let owner_attribute_types = owns.owner_attribute_types().clone();
        debug_assert!(attribute_types.len() > 0);

        let OwnsInstruction { owns, checks, .. } = owns;

        let iterate_mode = BinaryIterateMode::new(owns.owner(), owns.attribute(), &variable_modes, sort_by);
        let (sort_mode, output_tuple_positions) = sort_mode_and_tuple_positions(owns.owner(), owns.attribute(), sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_owns_filter_owner_attribute(owner_attribute_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_owns_filter_attribute(attribute_types.clone())
            }
        };

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
            owner_attribute_types,
            attribute_types,
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
        let filter_for_row: Box<OwnsFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });
        self.get_iterator_for(context, &self.variable_modes, self.sort_mode, self.tuple_positions.clone(), row, filter_for_row)
    }

    fn get_owns_for_owner(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        owner: Type,
    ) -> Result<BTreeSet<(ObjectType, AttributeType)>, Box<ConceptReadError>> {
        let object_type = match owner {
            Type::Entity(entity) => entity.into_object_type(),
            Type::Relation(relation) => relation.into_object_type(),
            _ => unreachable!("owner types must be relation or entity types"),
        };

        Ok(object_type
            .get_owned_attribute_types(snapshot, type_manager)?
            .into_iter()
            .map(|attribute_type| (object_type, attribute_type))
            .collect())
    }
}

impl fmt::Display for OwnsExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.owns, &self.iterate_mode)
    }
}

fn create_owns_filter_owner_attribute(owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((owner, attribute)) => match owner_attribute_types.get(&Type::from(*owner)) {
            Some(attribute_types) => Ok(attribute_types.contains(&Type::Attribute(*attribute))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_owns_filter_attribute(attribute_types: Arc<BTreeSet<Type>>) -> Arc<OwnsFilterFn> {
    Arc::new(move |result| match result {
        Ok((_, attribute)) => Ok(attribute_types.contains(&Type::Attribute(*attribute))),
        Err(err) => Err(err.clone()),
    })
}

impl DynamicBinaryIterator for OwnsExecutor {
    type Element = (ObjectType, AttributeType);

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.owns.owner()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.owns.attribute()
    }

    fn sort_mode(&self) -> BinaryTupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = owns_to_tuple_owner_attribute;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = owns_to_tuple_attribute_owner;

    fn get_iterator_unbound(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        _row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>> {
        let type_manager = context.type_manager();
        let owns: Vec<_> = self
            .owner_attribute_types
            .keys()
            .map(|owner| self.get_owns_for_owner(&*context.snapshot, type_manager, *owner))
            .try_collect()?;
        let iterator = owns.into_iter().flatten().map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_unbound_inverted(
        &self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<
        Either<UnreachableIteratorType<Self::Element>, UnreachableIteratorType<Self::Element>>,
        Box<ConceptReadError>,
    > {
        // is this ever relevant?
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
        }));
    }

    fn get_iterator_bound_from(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>> {
        let owner = type_from_row_or_annotations(self.owns.owner(), row, self.owner_attribute_types.keys());
        let type_manager = context.type_manager();
        let owns = self.get_owns_for_owner(&*context.snapshot, type_manager, owner)?;
        let iterator = owns.into_iter().sorted_by_key(|(owner, attribute)| (*attribute, *owner)).map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_check(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let owner = type_from_row_or_annotations(self.from(), row.as_reference(), self.owner_attribute_types.keys());
        let attribute = type_from_row_or_annotations(self.to(), row, self.attribute_types.iter());
        Ok(self.owner_attribute_types.get(&owner).unwrap().contains(&attribute)
            .then(|| (owner.as_object_type(), attribute.as_attribute_type())))
    }
}
