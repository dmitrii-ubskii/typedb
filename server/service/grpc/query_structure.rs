/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use compiler::query_structure::QueryStructure;
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::{label::Label, value::Value};
use ir::pattern::{
    constraint::{Constraint, IsaKind, SubKind},
    ParameterID, Vertex,
};
use storage::snapshot::ReadableSnapshot;
use typedb_protocol::query_structure::{query_constraint, query_vertex};
use answer::Type;

use crate::service::grpc::concept::{encode_attribute_type, encode_entity_type, encode_relation_type, encode_role_type, encode_type_concept, encode_value};

pub(crate) struct QueryStructureContext<'a, Snapshot: ReadableSnapshot> {
    pub(crate) query_structure: &'a QueryStructure,
    pub(crate) snapshot: &'a Snapshot,
    pub(crate) type_manager: &'a TypeManager,
    pub(crate) role_names: HashMap<Variable, String>,
}
impl<'a, Snapshot: ReadableSnapshot> QueryStructureContext<'a, Snapshot> {
    pub(crate) fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value(_, _)));
        self.query_structure.parameters.value(*param).cloned()
    }

    pub(crate) fn get_variable_name(&self, variable: &Variable) -> Option<String> {
        self.query_structure.variable_names.get(&variable).cloned()
    }

    pub(crate) fn get_type(&self, label: &Label) -> Option<answer::Type> {
        self.query_structure.parametrised_structure.resolved_labels.get(label).cloned()
    }

    pub(crate) fn get_call_syntax(&self, constraint: &Constraint<Variable>) -> Option<&String> {
        self.query_structure.parametrised_structure.calls_syntax.get(constraint)
    }

    pub(crate) fn get_role_type(&self, variable: &Variable) -> Option<&str> {
        self.role_names.get(variable).map(|name| name.as_str())
    }
}

pub(crate) fn encode_query_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
) -> Result<typedb_protocol::QueryStructure, Box<ConceptReadError>> {
    let branches = query_structure
        .parametrised_structure
        .branches
        .iter()
        .filter_map(|branch_opt| {
            branch_opt.as_ref().map(|branch| encode_query_branch(snapshot, type_manager, &query_structure, branch))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(typedb_protocol::QueryStructure { branches })
}

fn encode_query_branch(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
    branch: &[Constraint<Variable>],
) -> Result<typedb_protocol::query_structure::QueryBlock, Box<ConceptReadError>> {
    let mut constraints = Vec::new();
    let role_names = branch
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let context = QueryStructureContext { query_structure, snapshot, type_manager, role_names };
    branch.iter().enumerate().try_for_each(|(index, constraint)| {
        query_structure_constraint(&context, constraint, &mut constraints, index)
    })?;
    Ok(typedb_protocol::query_structure::QueryBlock { constraints })
}

fn query_structure_constraint(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    constraints: &mut Vec<typedb_protocol::query_structure::QueryConstraint>,
    index: usize,
) -> Result<(), Box<ConceptReadError>> {
    let span = constraint
        .source_span()
        .map(|span| query_constraint::ConstraintSpan { begin: span.begin_offset as u64, end: span.end_offset as u64 });
    match constraint {
        Constraint::Links(links) => {
            let relation = encode_query_vertex_variable(context, links.relation())?;
            let player = encode_query_vertex_variable(context, links.player())?;
            let role = encode_query_vertex_label_or_variable(context, links.role_type())?;
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Links(query_constraint::Links {
                    relation: Some(relation),
                    player: Some(player),
                    role: Some(role),
                    exactness: encode_exactness(false),
                })),
            });
        }
        Constraint::Has(has) => {
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Has(query_constraint::Has {
                    owner: Some(encode_query_vertex_variable(context, has.owner())?),
                    attribute: Some(encode_query_vertex_variable(context, has.attribute())?),
                    exactness: encode_exactness(false),
                })),
            });
        }

        Constraint::Isa(isa) => {
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Isa(query_constraint::Isa {
                    thing: Some(encode_query_vertex_variable(context, isa.thing())?),
                    r#type: Some(encode_query_vertex_label_or_variable(context, isa.type_())?),
                    exactness: encode_exactness(isa.isa_kind() == IsaKind::Exact),
                })),
            });
        }
        Constraint::Sub(sub) => {
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Sub(query_constraint::Sub {
                    subtype: Some(encode_query_vertex_label_or_variable(context, sub.subtype())?),
                    supertype: Some(encode_query_vertex_label_or_variable(context, sub.supertype())?),
                    exactness: encode_exactness(sub.sub_kind() == SubKind::Exact),
                })),
            });
        }
        Constraint::Owns(owns) => {
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Owns(query_constraint::Owns {
                    owner: Some(encode_query_vertex_label_or_variable(context, owns.owner())?),
                    attribute: Some(encode_query_vertex_label_or_variable(context, owns.attribute())?),
                    exactness: encode_exactness(false),
                })),
            });
        }
        Constraint::Relates(relates) => {
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Relates(query_constraint::Relates {
                    relation: Some(encode_query_vertex_label_or_variable(context, relates.relation())?),
                    role: Some(encode_query_vertex_label_or_variable(context, relates.role_type())?),
                    exactness: encode_exactness(false),
                })),
            });
        }
        Constraint::Plays(plays) => {
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Plays(query_constraint::Plays {
                    player: Some(encode_query_vertex_label_or_variable(context, plays.player())?),
                    role: Some(encode_query_vertex_label_or_variable(context, plays.role_type())?),
                    exactness: encode_exactness(false),
                })),
            });
        }
        //
        Constraint::IndexedRelation(indexed) => {
            let span_1 = indexed.source_span_1().map(|span| query_constraint::ConstraintSpan {
                begin: span.begin_offset as u64,
                end: span.end_offset as u64,
            });
            let span_2 = indexed.source_span_2().map(|span| query_constraint::ConstraintSpan {
                begin: span.begin_offset as u64,
                end: span.end_offset as u64,
            });
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span: span_1,
                constraint: Some(query_constraint::Constraint::Links(query_constraint::Links {
                    relation: Some(encode_query_vertex_variable(context, indexed.relation())?),
                    player: Some(encode_query_vertex_variable(context, indexed.player_1())?),
                    role: Some(encode_query_vertex_label_or_variable(context, indexed.role_type_1())?),
                    exactness: encode_exactness(false),
                })),
            });
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span: span_2,
                constraint: Some(query_constraint::Constraint::Links(query_constraint::Links {
                    relation: Some(encode_query_vertex_variable(context, indexed.relation())?),
                    player: Some(encode_query_vertex_variable(context, indexed.player_2())?),
                    role: Some(encode_query_vertex_label_or_variable(context, indexed.role_type_2())?),
                    exactness: encode_exactness(false),
                })),
            });
        }
        Constraint::ExpressionBinding(expr) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Expression#{index}"), |text| text.clone());
            let assigned = expr
                .ids_assigned()
                .map(|variable| encode_query_vertex_variable(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = expr
                .required_ids()
                .map(|variable| encode_query_vertex_variable(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::Expression(query_constraint::Expression {
                    text,
                    assigned,
                    arguments,
                })),
            });
        }
        Constraint::FunctionCallBinding(function_call) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Function#{index}"), |text| text.clone());
            let assigned = function_call
                .ids_assigned()
                .map(|variable| encode_query_vertex_variable(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = function_call
                .function_call()
                .argument_ids()
                .map(|variable| encode_query_vertex_value_or_variable(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(typedb_protocol::query_structure::QueryConstraint {
                span,
                constraint: Some(query_constraint::Constraint::FunctionCall(query_constraint::FunctionCall {
                    name: text,
                    assigned,
                    arguments,
                })),
            });
        }
        | Constraint::Comparison(_) => {}
        Constraint::RoleName(_) => {} // Handled separately via resolved_role_names

        // Constraints that probably don't need to be handled
        | Constraint::Kind(_)
        | Constraint::Label(_)
        | Constraint::Value(_)
        | Constraint::Is(_)
        | Constraint::Iid(_) => {}
        // Optimisations don't represent the structure
        Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => {}
    };
    Ok(())
}

fn encode_exactness(is_exact: bool) -> Option<query_constraint::ConstraintExactness> {
    let exactness = match is_exact {
        true => Some(query_constraint::constraint_exactness::Exactness::Exact(
            query_constraint::constraint_exactness::Exact {},
        )),
        false => Some(query_constraint::constraint_exactness::Exactness::Subtypes(
            query_constraint::constraint_exactness::Subtypes {},
        )),
    };
    Some(query_constraint::ConstraintExactness { exactness })
}

fn encode_query_vertex_variable(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<typedb_protocol::query_structure::QueryVertex, Box<ConceptReadError>> {
    let variable = vertex.as_variable().expect("Expected variable");
    let query_variable = query_vertex::QueryVariable {
        id: variable.id() as u32,
        named: context.get_variable_name(&variable).unwrap_or_else(|| variable.to_string()),
        in_answer: context.query_structure.available_variables.contains(&variable),
    };
    Ok(typedb_protocol::query_structure::QueryVertex {
        vertex: Some(query_vertex::Vertex::Variable(query_variable)),
    })
}

fn encode_query_vertex_label_or_variable(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<typedb_protocol::query_structure::QueryVertex, Box<ConceptReadError>> {
    match vertex {
        Vertex::Variable(_) => encode_query_vertex_variable(context, vertex),
        Vertex::Label(label) => {
            let type_ = context.get_type(label).expect("Expected all labels to be resolved");
            let encoded_type = encode_query_vertex_label(context.snapshot, context.type_manager, &type_)?;
            Ok(typedb_protocol::query_structure::QueryVertex {
                vertex: Some(query_vertex::Vertex::Label(encoded_type))
            })
        }
        Vertex::Parameter(_) => unreachable!("Expected variable or label"),
    }
}

fn encode_query_vertex_value_or_variable(
    context: &QueryStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<typedb_protocol::query_structure::QueryVertex, Box<ConceptReadError>> {
    match vertex {
        Vertex::Variable(_) => {
            encode_query_vertex_variable(context, vertex)
        }
        Vertex::Parameter(parameter) => {
            let value = context.get_parameter_value(&parameter).expect("Expected values to be present");
            Ok(typedb_protocol::query_structure::QueryVertex {
                vertex: Some(query_vertex::Vertex::Value(encode_value(value)))
            })
        }
        Vertex::Label(_) => unreachable!("Expected variable or value"),
    }
}

fn encode_query_vertex_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: &Type,
) -> Result<typedb_protocol::Type, Box<ConceptReadError>> {
    let encoded = match type_ {
        Type::Entity(entity) => {
            typedb_protocol::r#type::Type::EntityType(encode_entity_type(entity, snapshot, type_manager).unwrap() )
        }
        Type::Relation(relation) => {
            typedb_protocol::r#type::Type::RelationType(encode_relation_type(relation, snapshot, type_manager)?)
        }
        Type::Attribute(attribute) => {
            typedb_protocol::r#type::Type::AttributeType(encode_attribute_type(attribute, snapshot, type_manager)?)
        }
        Type::RoleType(role) => {
            typedb_protocol::r#type::Type::RoleType(encode_role_type(role, snapshot, type_manager)?)
        }
    };
    Ok(typedb_protocol::Type { r#type: Some(encoded) })
}