/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, fmt, iter, ops::Bound, sync::Arc, vec};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{executable::match_::instructions::thing::IsaReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    iterator::InstanceIterator,
    thing::{
        attribute::{Attribute, AttributeIterator},
        object::Object,
        thing_manager::ThingManager,
    },
};
use encoding::value::value::Value;
use ir::pattern::{
    constraint::{Isa, IsaKind},
    Vertex,
};
use itertools::Itertools;
use lending_iterator::adaptors::Filter;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    impl_becomes_sorted_tuple_iterator,
    instruction::{
        isa_executor::{
            AttributeEraseFn, IsaExecutor, IsaFilterMapFn, IsaTupleIterator, ObjectEraseFn, EXTRACT_THING, EXTRACT_TYPE,
        },
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, IsaToTupleFn, TuplePositions},
        type_from_row_or_annotations, BecomesSortedTupleIterator, BinaryIterateMode, Checker, DynamicBinaryIterator,
        FilterMapUnchangedFn, MapToTupleFn, TupleSortMode, UnreachableIteratorType, VariableModes, TYPES_EMPTY,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct IsaReverseExecutor {
    isa: Isa<ExecutorVariable>,
    sort_mode: TupleSortMode,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    type_to_instance_types: Arc<BTreeMap<Type, Vec<Type>>>,
    checker: Checker<(Thing, Type)>,
}

pub(crate) type IsaReverseBoundedSortedThing = IsaTupleIterator<MultipleTypeIsaIterator>;
pub(crate) type IsaReverseUnboundedSortedType = IsaTupleIterator<MultipleTypeIsaIterator>;
pub(crate) type IsaReverseUnified = IsaTupleIterator<MultipleTypeIsaIterator>;

type MultipleTypeIsaObjectIterator =
    iter::Flatten<vec::IntoIter<ThingWithType<iter::Map<InstanceIterator<Object>, ObjectEraseFn>>>>;
type MultipleTypeIsaAttributeIterator = iter::Flatten<
    vec::IntoIter<ThingWithType<iter::Map<AttributeIterator<InstanceIterator<Attribute>>, AttributeEraseFn>>>,
>;

pub(super) type MultipleTypeIsaIterator = iter::Chain<MultipleTypeIsaObjectIterator, MultipleTypeIsaAttributeIterator>;

type ThingWithType<I> = iter::Map<
    iter::Zip<I, iter::Repeat<Type>>,
    fn((Result<Thing, Box<ConceptReadError>>, Type)) -> Result<(Thing, Type), Box<ConceptReadError>>,
>;

impl IsaReverseExecutor {
    pub(crate) fn new(
        isa_reverse: IsaReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let IsaReverseInstruction { isa, checks, type_to_instance_types, .. } = isa_reverse;
        debug_assert!(type_to_instance_types.len() > 0);
        debug_assert!(!type_to_instance_types.iter().any(|(type_, _)| matches!(type_, Type::RoleType(_))));
        let iterate_mode = BinaryIterateMode::new(isa.type_(), isa.thing(), &variable_modes, sort_by);

        let thing = isa.thing().as_variable();
        let type_ = isa.type_().as_variable();
        let sort_mode = if type_.as_ref().unwrap() == &sort_by { TupleSortMode::From } else { TupleSortMode::To };
        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([type_, thing]),
            _ => TuplePositions::Pair([thing, type_]),
        };

        let checker = Checker::<(Thing, Type)>::new(
            checks,
            [(thing, EXTRACT_THING), (type_, EXTRACT_TYPE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect(),
        );

        Self {
            isa,
            sort_mode,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            type_to_instance_types,
            checker,
        }
    }

    pub(crate) fn get_iterator(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<TupleIterator, Box<ConceptReadError>> {
        let check = self.checker.filter_for_row(context, &row);
        let filter_for_row: Box<IsaFilterMapFn> = Box::new(move |item| match check(&item) {
            Ok(true) | Err(_) => Some(item),
            Ok(false) => None,
        });
        self.get_iterator_for(context, &self.variable_modes, self.sort_mode, row, filter_for_row)
    }
}

impl fmt::Display for IsaReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.isa, &self.iterate_mode)
    }
}

fn with_type<I: Iterator<Item = Result<Thing, Box<ConceptReadError>>>>(iter: I, type_: Type) -> ThingWithType<I> {
    iter.zip(iter::repeat(type_)).map(|(thing_res, ty)| match thing_res {
        Ok(thing) => Ok((thing, ty)),
        Err(err) => Err(err),
    })
}

pub(super) fn instances_of_types_chained<'a>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    types: impl Iterator<Item = &'a Type>,
    type_to_instance_types: &BTreeMap<Type, Vec<Type>>,
    isa_kind: IsaKind,
    range: &(Bound<Value<'_>>, Bound<Value<'_>>),
) -> Result<MultipleTypeIsaIterator, Box<ConceptReadError>> {
    let (attribute_types, object_types) =
        types.into_iter().partition::<Vec<_>, _>(|type_| matches!(type_, Type::Attribute(_)));

    let object_iters: Vec<ThingWithType<iter::Map<InstanceIterator<Object>, ObjectEraseFn>>> = object_types
        .into_iter()
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(type_).unwrap_or(&TYPES_EMPTY).clone()
            } else {
                vec![*type_]
            };
            returned_types.into_iter().map(move |subtype| {
                Ok::<_, Box<_>>(with_type(
                    thing_manager
                        .get_objects_in(snapshot, subtype.as_object_type())
                        .map((|res| res.map(Thing::from)) as ObjectEraseFn),
                    *type_,
                ))
            })
        })
        .try_collect()?;
    let object_iter: MultipleTypeIsaObjectIterator = object_iters.into_iter().flatten();

    // TODO: don't unwrap inside the operators
    let attribute_iters: Vec<_> = attribute_types
        .into_iter()
        .flat_map(|type_| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) {
                type_to_instance_types.get(type_).unwrap_or(&TYPES_EMPTY).clone()
            } else {
                vec![*type_]
            };
            returned_types.into_iter().map(move |subtype| {
                Ok::<_, Box<_>>(with_type(
                    thing_manager
                        .get_attributes_in_range(snapshot, subtype.as_attribute_type(), range)?
                        .map((|res| res.map(Thing::Attribute)) as AttributeEraseFn),
                    *type_,
                ))
            })
        })
        .try_collect()?;
    let attribute_iter: MultipleTypeIsaAttributeIterator = attribute_iters.into_iter().flatten();

    let thing_iter: MultipleTypeIsaIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}

impl DynamicBinaryIterator for IsaReverseExecutor {
    type Element = (Thing, Type);
    type IteratorUnbound = MultipleTypeIsaIterator;
    type IteratorUnboundInverted = UnreachableIteratorType;
    type IteratorUnboundInvertedMerged = UnreachableIteratorType;
    type IteratorBoundFrom = MultipleTypeIsaIterator;

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.isa.type_()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.isa.thing()
    }

    fn sort_mode(&self) -> TupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = IsaExecutor::TUPLE_TO_FROM;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = IsaExecutor::TUPLE_FROM_TO;

    fn get_iterator_unbound(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<Self::IteratorUnbound, Box<ConceptReadError>> {
        let range =
            self.checker.value_range_for(context, Some(row.as_reference()), self.isa.thing().as_variable().unwrap())?;
        instances_of_types_chained(
            &*context.snapshot,
            &*context.thing_manager,
            self.type_to_instance_types.keys(),
            self.type_to_instance_types.as_ref(),
            self.isa.isa_kind(),
            &range,
        )
    }

    fn get_iterator_unbound_inverted(
        &self,
        _context: &ExecutionContext<impl ReadableSnapshot + Sized>,
    ) -> Result<Either<Self::IteratorUnboundInverted, Self::IteratorUnboundInvertedMerged>, Box<ConceptReadError>> {
        unreachable!()
    }

    fn get_iterator_bound_from(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
        from: &VariableValue<'_>,
    ) -> Result<Self::IteratorBoundFrom, Box<ConceptReadError>> {
        let range =
            self.checker.value_range_for(context, Some(row.as_reference()), self.isa.thing().as_variable().unwrap())?;
        let type_ = type_from_row_or_annotations(self.isa.type_(), row, self.type_to_instance_types.keys());
        debug_assert!(&VariableValue::Type(type_) == from);
        instances_of_types_chained(
            &*context.snapshot,
            &*context.thing_manager,
            [&type_].into_iter(),
            self.type_to_instance_types.as_ref(),
            self.isa.isa_kind(),
            &range,
        )
        //  TODO: I don't see this as being needed. Verify with a debug assert in the pre-refactor setting
        // iterator
        //     .filter_map(Box::new(move |res| match res {
        //         Ok((_, ty)) if ty == type_ => Some(Ok(res)),
        //         Ok(_) => None,
        //         Err(err) => Some(Err(err)),
        //     }) as _)
        //
    }

    fn get_iterator_check(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        from: &VariableValue<'_>,
        to: &VariableValue<'_>,
    ) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let VariableValue::Type(type_) = from else { panic!() };
        let VariableValue::Thing(thing) = to else { panic!() };
        Ok((&thing.type_() == type_).then(|| (thing.clone(), type_.clone())))
    }
}

impl_becomes_sorted_tuple_iterator! {
    MultipleTypeIsaIterator[(Thing, Type)] => IsaReverseUnified,
}
