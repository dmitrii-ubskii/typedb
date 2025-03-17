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
use compiler::{executable::match_::instructions::type_::PlaysReverseInstruction, ExecutorVariable};
use concept::{
    error::ConceptReadError,
    type_::{object_type::ObjectType, role_type::RoleType},
};
use itertools::Itertools;
use concept::type_::PlayerAPI;
use ir::pattern::Vertex;
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;

use crate::{
    instruction::{
        iterator::{SortedTupleIterator, TupleIterator},
        plays_executor::{
            PlaysFilterFn, PlaysFilterMapFn, PlaysTupleIterator, PlaysVariableValueExtractor, EXTRACT_PLAYER,
            EXTRACT_ROLE,
        },
        relates_executor::RelatesExecutor,
        tuple::{plays_to_tuple_player_role, plays_to_tuple_role_player, TuplePositions},
        type_from_row_or_annotations, BinaryIterateMode, Checker, VariableModes,
    },
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
};
use crate::instruction::helpers::{DynamicBinaryIterator, ExecutorIteratorBoundFrom, ExecutorIteratorUnbound, ExecutorIteratorUnboundInverted, UnreachableIteratorType};
use crate::instruction::{MapToTupleFn, TupleSortMode};
use crate::instruction::plays_executor::PlaysExecutor;

pub(crate) struct PlaysReverseExecutor {
    plays: ir::pattern::constraint::Plays<ExecutorVariable>,
    iterate_mode: BinaryIterateMode,
    variable_modes: VariableModes,
    tuple_positions: TuplePositions,
    role_player_types: Arc<BTreeMap<Type, Vec<Type>>>,
    player_types: Arc<BTreeSet<Type>>,
    filter_fn: Arc<PlaysFilterFn>,
    checker: Checker<(ObjectType, RoleType)>,
}

impl fmt::Debug for PlaysReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PlaysReverseExecutor")
    }
}

pub(super) type PlaysReverseUnboundedSortedRole = PlaysTupleIterator<
    iter::Map<
        iter::Flatten<vec::IntoIter<BTreeSet<(ObjectType, RoleType)>>>,
        fn((ObjectType, RoleType)) -> Result<(ObjectType, RoleType), Box<ConceptReadError>>,
    >,
>;
pub(super) type PlaysReverseBoundedSortedPlayer = PlaysTupleIterator<
    iter::Map<
        vec::IntoIter<(ObjectType, RoleType)>,
        fn((ObjectType, RoleType)) -> Result<(ObjectType, RoleType), Box<ConceptReadError>>,
    >,
>;

impl PlaysReverseExecutor {
    pub(crate) fn new(
        plays: PlaysReverseInstruction<ExecutorVariable>,
        variable_modes: VariableModes,
        sort_by: ExecutorVariable,
    ) -> Self {
        let arc = plays.player_types().clone();
        let player_types = arc;
        let role_player_types = plays.role_player_types().clone();
        debug_assert!(player_types.len() > 0);

        let PlaysReverseInstruction { plays, checks, .. } = plays;

        let iterate_mode = BinaryIterateMode::new(plays.role_type(), plays.player(), &variable_modes, sort_by);
        let filter_fn = match iterate_mode {
            BinaryIterateMode::Unbound => create_plays_filter_player_role(role_player_types.clone()),
            BinaryIterateMode::UnboundInverted | BinaryIterateMode::BoundFrom => {
                create_plays_filter_role(player_types.clone())
            }
        };

        let player = plays.player().as_variable();
        let role_type = plays.role_type().as_variable();

        let output_tuple_positions = match iterate_mode {
            BinaryIterateMode::Unbound => TuplePositions::Pair([role_type, player]),
            _ => TuplePositions::Pair([player, role_type]),
        };

        let checker = Checker::<(ObjectType, RoleType)>::new(
            checks,
            [(player, EXTRACT_PLAYER), (role_type, EXTRACT_ROLE)]
                .into_iter()
                .filter_map(|(var, ex)| Some((var?, ex)))
                .collect::<HashMap<ExecutorVariable, PlaysVariableValueExtractor>>(),
        );

        Self {
            plays,
            iterate_mode,
            variable_modes,
            tuple_positions: output_tuple_positions,
            role_player_types,
            player_types,
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
        let filter_for_row: Box<PlaysFilterMapFn> = Box::new(move |item| match filter(&item) {
            Ok(true) => match check(&item) {
                Ok(true) | Err(_) => Some(item),
                Ok(false) => None,
            },
            Ok(false) => None,
            Err(_) => Some(item),
        });
        self.get_iterator_for(context, &self.variable_modes, todo!("self.sort_mode"), row, filter_for_row)
    }
}

impl fmt::Display for PlaysReverseExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Reverse[{}], mode={}", &self.plays, &self.iterate_mode)
    }
}

fn create_plays_filter_player_role(role_player_types: Arc<BTreeMap<Type, Vec<Type>>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok((player, role)) => match role_player_types.get(&Type::RoleType(*role)) {
            Some(player_types) => Ok(player_types.contains(&Type::from(*player))),
            None => Ok(false),
        },
        Err(err) => Err(err.clone()),
    })
}

fn create_plays_filter_role(player_types: Arc<BTreeSet<Type>>) -> Arc<PlaysFilterFn> {
    Arc::new(move |result| match result {
        Ok((player, _)) => Ok(player_types.contains(&Type::from(*player))),
        Err(err) => Err(err.clone()),
    })
}

impl DynamicBinaryIterator for PlaysReverseExecutor {
    type Element = (ObjectType, RoleType);

    fn from(&self) -> &Vertex<ExecutorVariable> {
        self.plays.role_type()
    }

    fn to(&self) -> &Vertex<ExecutorVariable> {
        self.plays.player()
    }

    fn sort_mode(&self) -> TupleSortMode {
        todo!()
    }

    const TUPLE_FROM_TO: MapToTupleFn<Self::Element> = PlaysExecutor::TUPLE_TO_FROM;
    const TUPLE_TO_FROM: MapToTupleFn<Self::Element> = PlaysExecutor::TUPLE_FROM_TO;

    fn get_iterator_unbound(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorUnbound<Self>, Box<ConceptReadError>> {
        let type_manager = context.type_manager();
        let plays: Vec<_> = self
            .role_player_types
            .keys()
            .map(|role| {
                let role_type = role.as_role_type();
                role_type
                    .get_player_types(&*context.snapshot, type_manager)
                    .map(|res| res.to_owned().keys().map(|object_type| (*object_type, role_type)).collect())
            })
            .try_collect()?;
        let iterator = plays.into_iter().flatten().map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_unbound_inverted(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>) -> Result<Either<UnreachableIteratorType<Self::Element>, UnreachableIteratorType<Self::Element>>, Box<ConceptReadError>> {
        return Err(Box::new(ConceptReadError::UnimplementedFunctionality {
            functionality: error::UnimplementedFeature::IrrelevantUnboundInvertedMode(file!()),
        }));
    }

    fn get_iterator_bound_from(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<impl ExecutorIteratorBoundFrom<Self>, Box<ConceptReadError>> {
        let role_type =
            type_from_row_or_annotations(self.plays.role_type(), row, self.role_player_types.keys())
                .as_role_type();
        let type_manager = context.type_manager();
        let plays = role_type
            .get_player_types(&*context.snapshot, type_manager)?
            .to_owned()
            .into_keys()
            .map(|object_type| (object_type, role_type));

        let iterator = plays.into_iter().sorted_by_key(|&(player, _)| player).map(Ok as _);
        Ok(iterator)
    }

    fn get_iterator_check(&self, context: &ExecutionContext<impl ReadableSnapshot + Sized>, row: MaybeOwnedRow<'_>) -> Result<Option<Self::Element>, Box<ConceptReadError>> {
        let role = type_from_row_or_annotations(self.from(), row.as_reference(), self.role_player_types.keys());
        let player = type_from_row_or_annotations(self.to(), row, self.player_types.iter());
        Ok(self.role_player_types.get(&role).unwrap().contains(&player)
            .then(|| (player.as_object_type(), role.as_role_type())))
    }
}