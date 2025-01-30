/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::type_::type_manager::TypeManager;
use ir::pattern::{
    conjunction::Conjunction,
    constraint::Constraint,
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    annotation::{
        pipeline::{AnnotatedPipeline, AnnotatedStage},
        type_annotations::TypeAnnotations,
    },
    transformation::{relation_index::relation_index_transformation, StaticOptimiserError},
};

pub fn apply_transformations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline: &mut AnnotatedPipeline,
) -> Result<(), StaticOptimiserError> {
    for stage in &mut pipeline.annotated_stages {
        if let AnnotatedStage::Match { block, block_annotations, .. } = stage {
            prune_redundant_roleplayer_deduplication(block.conjunction_mut(), block_annotations);
            relation_index_transformation(block.conjunction_mut(), block_annotations, type_manager, snapshot)?;
        }
    }
    Ok(())

    // Ideas:
    // - we should move subtrees/graphs of a query that have no returned variables into a new pattern: "Check", which are only checked for a single answer
    // - we should push constraints, like comparisons, that apply to variables passed into functions, into the function itself
    // - function inlining v1: if a function does not have recursion or sort/offset/limit, we could inline the function into the query
    // - function inlining v2: we could try to inline/lift some constraints from recursive calls into the parent query to dramatically cut the search space
    // - function inlining v3: we could introduce new sub-patterns that include sort/offset/limit that let us more generally inline functions?
}

fn prune_redundant_roleplayer_deduplication(conjunction: &mut Conjunction, block_annotations: &mut TypeAnnotations) {
    conjunction.constraints_mut().constraints_mut().retain(|constraint| {
        if let Constraint::LinksDeduplication(dedup) = constraint {
            let first = block_annotations
                .constraint_annotations_of(Constraint::Links(dedup.links1().clone()))
                .unwrap()
                .as_links()
                .player_to_role();
            let second = block_annotations
                .constraint_annotations_of(Constraint::Links(dedup.links2().clone()))
                .unwrap()
                .as_links()
                .player_to_role();
            first.iter().any(|(player, role_types)| {
                return second
                    .get(&player)
                    .map(|type_set| role_types.iter().any(|role_type| type_set.contains(role_type)))
                    .unwrap_or(false);
            })
        } else {
            true
        }
    });
}
