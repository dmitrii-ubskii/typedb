/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::BTreeMap, fmt, iter, ops::Bound, sync::Arc, vec};

use answer::{variable_value::VariableValue, Thing, Type};
use compiler::{executable::match_::instructions::thing::IsaInstruction, ExecutorVariable};
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
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    impl_becomes_sorted_tuple_iterator,
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        tuple::{isa_to_tuple_thing_type, isa_to_tuple_type_thing, IsaToTupleFn, TuplePositions},
        BecomesSortedTupleIterator, BinaryIterateMode, Checker, DynamicBinaryIterator, FilterMapUnchangedFn,
        MapToTupleFn, TupleSortMode, UnreachableIteratorType, VariableModes, TYPES_EMPTY,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};

#[derive(Debug)]
pub(crate) struct IsaExecutor {
    isa: Isa<ExecutorVariable>,
    sort_mode: TupleSortMode,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    instance_type_to_types: Arc<BTreeMap<Type, Vec<Type>>>,
    checker: Checker<(Thing, Type)>,
}

type MultipleTypeIsaObjectIterator =
    iter::Flatten<vec::IntoIter<ThingWithTypes<iter::Map<InstanceIterator<Object>, ObjectEraseFn>>>>;
type MultipleTypeIsaAttributeIterator = iter::Flatten<
    vec::IntoIter<ThingWithTypes<iter::Map<AttributeIterator<InstanceIterator<Attribute>>, AttributeEraseFn>>>,
>;

pub(super) type MultipleTypeIsaIterator = iter::Chain<MultipleTypeIsaObjectIterator, MultipleTypeIsaAttributeIterator>;

pub(super) type IsaTupleIterator<I> = iter::Map<iter::FilterMap<I, Box<IsaFilterMapFn>>, IsaToTupleFn>;

type ThingWithTypes<I> = iter::FlatMap<
    iter::Zip<I, iter::Repeat<Vec<Type>>>,
    Vec<Result<(Thing, Type), Box<ConceptReadError>>>,
    fn((Result<Thing, Box<ConceptReadError>>, Vec<Type>)) -> Vec<Result<(Thing, Type), Box<ConceptReadError>>>,
>;

pub(super) type IsaUnboundedSortedThing = IsaTupleIterator<MultipleTypeIsaIterator>;

pub(super) type IsaBoundedSortedType =
    IsaTupleIterator<ThingWithTypes<iter::Once<Result<Thing, Box<ConceptReadError>>>>>;

pub(super) type ObjectEraseFn =
    for<'a> fn(Result<Object, Box<ConceptReadError>>) -> Result<Thing, Box<ConceptReadError>>;
pub(super) type AttributeEraseFn =
    for<'a> fn(Result<Attribute, Box<ConceptReadError>>) -> Result<Thing, Box<ConceptReadError>>;

pub(super) type IsaFilterMapFn = FilterMapUnchangedFn<(Thing, Type)>;

type IsaVariableValueExtractor = for<'a, 'b> fn(&'a (Thing, Type)) -> VariableValue<'a>;

pub(super) const EXTRACT_THING: IsaVariableValueExtractor = |(thing, _)| VariableValue::Thing(thing.clone());
pub(super) const EXTRACT_TYPE: IsaVariableValueExtractor = |&(_, type_)| VariableValue::Type(type_);

impl IsaExecutor {
    pub(crate) fn new(
        isa: IsaInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let IsaInstruction { isa, checks, instance_type_to_types, .. } = isa;
        debug_assert!(instance_type_to_types.len() > 0);
        let iterate_mode = BinaryIterateMode::new(isa.thing(), isa.type_(), &variable_modes, sort_by);

        let thing = isa.thing().as_variable();
        let type_ = isa.type_().as_variable();
        let sort_mode = if thing.as_ref().unwrap() == &sort_by { TupleSortMode::From } else { TupleSortMode::To };
        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([thing, type_]),
            _ => TuplePositions::Pair([type_, thing]),
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
            instance_type_to_types,
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

impl fmt::Display for IsaExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}], mode={}", &self.isa, &self.iterate_mode)
    }
}

fn with_types<I: Iterator<Item = Result<Thing, Box<ConceptReadError>>>>(
    iter: I,
    types: Vec<Type>,
) -> ThingWithTypes<I> {
    iter.zip(iter::repeat(types)).flat_map(|(thing_res, types)| match thing_res {
        Ok(thing) => types.into_iter().map(|ty| Ok((thing.clone(), ty))).collect(),
        Err(err) => vec![Err(err.clone())],
    })
}

pub(super) fn instances_of_all_types_chained(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    instance_types_to_types: &BTreeMap<Type, Vec<Type>>,
    isa_kind: IsaKind,
    instance_values_range: (Bound<Value<'_>>, Bound<Value<'_>>),
) -> Result<MultipleTypeIsaIterator, Box<ConceptReadError>> {
    // TODO: this method contains a lot of heap allocations - we clone the Vec<Type> each time!

    // object types and attribute types will continue to be sorted, based on their source in the BTreeMap
    let (attribute_types, object_types) =
        instance_types_to_types.iter().partition::<Vec<_>, _>(|(type_, _)| matches!(type_, Type::Attribute(_)));

    let object_iters: Vec<_> = object_types
        .into_iter()
        .map(|(type_, types)| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) { types.clone() } else { vec![*type_] };
            Ok::<_, Box<_>>(with_types(
                thing_manager
                    .get_objects_in(snapshot, type_.as_object_type())
                    .map((|res| res.map(Thing::from)) as ObjectEraseFn),
                returned_types,
            ))
        })
        .try_collect()?;
    // Since the object types are sorted, and instance ordering follows matches type ordering, we have instance-sorting here
    let object_iter: MultipleTypeIsaObjectIterator = object_iters.into_iter().flatten();

    let attribute_iters: Vec<_> = attribute_types
        .into_iter()
        .map(|(type_, types)| {
            let returned_types = if matches!(isa_kind, IsaKind::Subtype) { types.clone() } else { vec![*type_] };
            Ok::<_, Box<_>>(with_types(
                thing_manager
                    .get_attributes_in_range(snapshot, type_.as_attribute_type(), &instance_values_range)?
                    .map((|res| res.map(Thing::Attribute)) as AttributeEraseFn),
                returned_types,
            ))
        })
        .try_collect()?;
    let attribute_iter: MultipleTypeIsaAttributeIterator = attribute_iters.into_iter().flatten();

    let thing_iter: MultipleTypeIsaIterator = object_iter.chain(attribute_iter);
    Ok(thing_iter)
}

type IsaBoundedInner = ThingWithTypes<iter::Once<Result<Thing, Box<ConceptReadError>>>>;
impl DynamicBinaryIterator for IsaExecutor {
    type Element = (Thing, Type);
    type IteratorUnbound = MultipleTypeIsaIterator;
    type IteratorUnboundInverted = UnreachableIteratorType;
    type IteratorUnboundInvertedMerged = UnreachableIteratorType;
    type IteratorBoundFrom = IsaBoundedInner;

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.isa.thing()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.isa.type_()
    }

    fn sort_mode(&self) -> TupleSortMode {
        self.sort_mode
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = isa_to_tuple_thing_type;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = isa_to_tuple_type_thing;

    fn get_iterator_unbound(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        row: MaybeOwnedRow<'_>,
    ) -> Result<Self::IteratorUnbound, Box<ConceptReadError>> {
        let instances_range = if let Vertex::Variable(thing_variable) = self.isa.thing() {
            self.checker.value_range_for(context, Some(row.as_reference()), *thing_variable)?
        } else {
            (Bound::Unbounded, Bound::Unbounded)
        };
        instances_of_all_types_chained(
            &*context.snapshot,
            &*context.thing_manager,
            self.instance_type_to_types.as_ref(),
            self.isa.isa_kind(),
            instances_range,
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
        let VariableValue::Thing(thing) = from.to_owned() else {
            unreachable!("Has thing must be an entity or relation.")
        };
        let type_ = thing.type_();
        let supertypes = self.instance_type_to_types.get(&type_).cloned().unwrap_or(TYPES_EMPTY);
        Ok(with_types(iter::once(Ok(thing)), supertypes))
    }

    fn get_iterator_check(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + Sized>,
        from: &VariableValue<'_>,
        to: &VariableValue<'_>,
    ) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        todo!()
    }
}

impl_becomes_sorted_tuple_iterator! {
    MultipleTypeIsaIterator[(Thing, Type)] => IsaUnbounded,
    IsaBoundedInner[(Thing, Type)] => IsaBounded,
}
