/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
    iter::zip,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::definition::definition_key::DefinitionKey,
    value::{label::Label, value_type::ValueType},
};
use ir::{
    pattern::Vertex,
    pipeline::{
        function::{Function, FunctionBody, ReturnOperation},
        function_signature::FunctionID,
        ParameterRegistry, VariableRegistry,
    },
    translation::tokens::translate_value_type,
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;
use typeql::{
    schema::definable::function::{Output, SingleSelector},
    type_::NamedType,
    TypeRef, TypeRefAny,
};

use crate::{
    annotation::{
        expression::compiled_expression::ExpressionValueType,
        pipeline::{annotate_pipeline_stages, resolve_reducer_by_value_type, AnnotatedStage},
        type_seeder, FunctionAnnotationError, TypeInferenceError,
    },
    executable::reduce::ReduceInstruction,
};

#[derive(Debug, Clone)]
pub enum FunctionParameterAnnotation {
    Concept(BTreeSet<Type>),
    Value(ValueType),
}

#[derive(Debug, Clone)]
pub struct AnnotatedFunction {
    pub variable_registry: VariableRegistry,
    pub parameter_registry: ParameterRegistry,
    pub arguments: Vec<Variable>,
    pub argument_annotations: Vec<FunctionParameterAnnotation>,
    pub stages: Vec<AnnotatedStage>,
    pub return_: AnnotatedFunctionReturn,
}

impl AnnotatedFunction {
    pub(crate) fn get_annotated_signature(&self) -> AnnotatedFunctionSignature {
        let returned = Vec::from(self.return_.annotations());
        AnnotatedFunctionSignature { returned, arguments: self.argument_annotations.clone() }
    }
}

pub type AnnotatedPreambleFunctions = Vec<AnnotatedFunction>; // TODO
pub type AnnotatedSchemaFunctions = HashMap<DefinitionKey<'static>, AnnotatedFunction>; // TODO

#[derive(Debug, Clone)]
pub enum AnnotatedFunctionReturn {
    Stream { variables: Vec<Variable>, annotations: Vec<FunctionParameterAnnotation> },
    Single { selector: SingleSelector, variables: Vec<Variable>, annotations: Vec<FunctionParameterAnnotation> },
    ReduceCheck {},
    ReduceReducer { instructions: Vec<ReduceInstruction<Variable>> },
}

impl AnnotatedFunctionReturn {
    pub(crate) fn referenced_variables(&self) -> Vec<Variable> {
        match self {
            AnnotatedFunctionReturn::Stream { variables, .. } => variables.clone(),
            AnnotatedFunctionReturn::Single { variables, .. } => variables.clone(),
            AnnotatedFunctionReturn::ReduceCheck { .. } => Vec::new(),
            AnnotatedFunctionReturn::ReduceReducer { instructions } => {
                instructions.iter().filter_map(|x| x.id()).collect()
            }
        }
    }
}

impl AnnotatedFunctionReturn {
    pub fn annotations(&self) -> Cow<'_, [FunctionParameterAnnotation]> {
        match self {
            AnnotatedFunctionReturn::Stream { annotations, .. } => Cow::Borrowed(annotations),
            AnnotatedFunctionReturn::Single { annotations, .. } => Cow::Borrowed(annotations),
            AnnotatedFunctionReturn::ReduceCheck { .. } => {
                Cow::Borrowed(&[FunctionParameterAnnotation::Value(ValueType::Boolean)])
            }
            AnnotatedFunctionReturn::ReduceReducer { instructions } => Cow::Owned(
                instructions
                    .iter()
                    .map(|instruction| FunctionParameterAnnotation::Value(instruction.output_type()))
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnnotatedFunctionSignature {
    pub arguments: Vec<FunctionParameterAnnotation>,
    pub returned: Vec<FunctionParameterAnnotation>,
}

#[derive(Debug)]
pub struct AnnotatedFunctionSignatures {
    schema_functions: HashMap<DefinitionKey<'static>, AnnotatedFunctionSignature>,
    local_functions: Vec<AnnotatedFunctionSignature>,
}

impl AnnotatedFunctionSignatures {
    pub(crate) fn new(
        schema_functions: HashMap<DefinitionKey<'static>, AnnotatedFunctionSignature>,
        local_functions: Vec<AnnotatedFunctionSignature>,
    ) -> Self {
        Self { schema_functions, local_functions }
    }

    pub(crate) fn get(&self, function_id: &FunctionID) -> Option<&AnnotatedFunctionSignature> {
        match function_id {
            FunctionID::Schema(definition_key) => self.schema_functions.get(definition_key),
            FunctionID::Preamble(index) => self.local_functions.get(index.clone()),
        }
    }
}

pub fn annotate_stored_functions<'a>(
    functions: &mut HashMap<DefinitionKey<'static>, Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<AnnotatedSchemaFunctions, Box<FunctionAnnotationError>> {
    let label_based_signature_annotations_as_map = functions
        .iter()
        .map(|(id, function)| {
            annotate_signature_based_on_labels(snapshot, type_manager, function).map(|x| (id.clone(), x))
        })
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let seed_signature_annotations =
        AnnotatedFunctionSignatures::new(label_based_signature_annotations_as_map, Vec::new());
    let preliminary_signature_annotations = functions
        .iter_mut()
        .map(|(function_id, function)| {
            annotate_named_function(function, snapshot, type_manager, &seed_signature_annotations)
                .map(|annotated| (function_id.clone(), annotated.get_annotated_signature()))
        })
        .collect::<Result<HashMap<DefinitionKey<'static>, AnnotatedFunctionSignature>, Box<FunctionAnnotationError>>>(
        )?;
    let preliminary_signature_annotations =
        AnnotatedFunctionSignatures::new(preliminary_signature_annotations, Vec::new());
    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let annotated_functions = functions
        .iter_mut()
        .map(|(function_id, function)| {
            annotate_named_function(function, snapshot, type_manager, &preliminary_signature_annotations)
                .map(|annotated| (function_id.clone(), annotated))
        })
        .collect::<Result<HashMap<DefinitionKey<'static>, AnnotatedFunction>, Box<FunctionAnnotationError>>>()?;

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the graphs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    Ok(annotated_functions)
}

pub fn annotate_preamble_functions(
    mut functions: Vec<Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_function_signatures: HashMap<DefinitionKey<'static>, AnnotatedFunctionSignature>,
) -> Result<AnnotatedPreambleFunctions, Box<FunctionAnnotationError>> {
    let preamble_annotations_from_labels_as_map = functions
        .iter()
        .map(|function| annotate_signature_based_on_labels(snapshot, type_manager, function))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let label_based_signature_annotations =
        AnnotatedFunctionSignatures::new(schema_function_signatures.clone(), preamble_annotations_from_labels_as_map);
    let preliminary_signature_annotations_as_map = functions
        .iter_mut()
        .map(|function| {
            Ok(annotate_named_function(function, snapshot, type_manager, &label_based_signature_annotations)?
                .get_annotated_signature())
        })
        .collect::<Result<Vec<AnnotatedFunctionSignature>, Box<FunctionAnnotationError>>>()?;
    // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    let preliminary_signature_annotations =
        AnnotatedFunctionSignatures::new(schema_function_signatures, preliminary_signature_annotations_as_map);
    let annotated_functions = functions
        .iter_mut()
        .map(|function| annotate_named_function(function, snapshot, type_manager, &preliminary_signature_annotations))
        .collect::<Result<Vec<AnnotatedFunction>, Box<FunctionAnnotationError>>>()?;

    // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the graphs, and restart at the source of any SCC.
    // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // Further, In a chain of three functions where the first two bodies have no function calls
    // but rely on the third function to infer annotations, the annotations will not reach the first function.
    Ok(annotated_functions)
}

pub(crate) fn annotate_anonymous_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &AnnotatedFunctionSignatures,
    caller_type_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    caller_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function { arguments, argument_labels, .. } = function;
    debug_assert!(argument_labels.is_none());
    let mut argument_concept_variable_types = BTreeMap::new();
    let mut argument_value_variable_types = BTreeMap::new();
    for var in arguments {
        if let Some(concept_annotation) = caller_type_annotations.get(var) {
            argument_concept_variable_types.insert(*var, concept_annotation.clone());
        } else if let Some(value_annotation) = caller_value_type_annotations.get(var) {
            argument_value_variable_types.insert(*var, value_annotation.clone());
        } else {
            unreachable!("The type annotations for the argument in the function call should be known by now")
        }
    }
    annotate_function_impl(
        function,
        snapshot,
        type_manager,
        annotated_function_signatures,
        argument_concept_variable_types,
        argument_value_variable_types,
    )
}

pub(super) fn annotate_named_function(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &AnnotatedFunctionSignatures,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function { arguments, argument_labels, .. } = function;
    debug_assert!(argument_labels.is_some());
    let mut argument_concept_variable_types = BTreeMap::new();
    let mut argument_value_variable_types = BTreeMap::new();
    for (arg_index, (var, label)) in zip(arguments, argument_labels.as_ref().unwrap()).enumerate() {
        match get_argument_annotations_from_labels(snapshot, type_manager, label, arg_index)? {
            FunctionParameterAnnotation::Concept(concept_annotation) => {
                argument_concept_variable_types.insert(*var, Arc::new(concept_annotation));
            }
            FunctionParameterAnnotation::Value(value_annotation) => {
                argument_value_variable_types.insert(*var, ExpressionValueType::Single(value_annotation));
            }
        }
    }
    annotate_function_impl(
        function,
        snapshot,
        type_manager,
        annotated_function_signatures,
        argument_concept_variable_types,
        argument_value_variable_types,
    )
}

fn annotate_function_impl(
    function: &mut Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotated_function_signatures: &AnnotatedFunctionSignatures,
    argument_concept_variable_types: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    argument_value_variable_types: BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunction, Box<FunctionAnnotationError>> {
    let Function {
        name, context, parameters, function_body: FunctionBody { stages, return_operation }, arguments, ..
    } = function;

    let (stages, running_variable_types, running_value_types) = annotate_pipeline_stages(
        snapshot,
        type_manager,
        annotated_function_signatures,
        &mut context.variable_registry,
        &parameters,
        stages.clone(),
        argument_concept_variable_types,
        argument_value_variable_types.clone(),
    )
    .map_err(|err| {
        Box::new(FunctionAnnotationError::TypeInference { name: name.to_string(), typedb_source: Box::new(err) })
    })?;

    let return_ = annotate_return(
        snapshot,
        type_manager,
        &context.variable_registry,
        return_operation,
        &running_variable_types,
        &running_value_types,
    )?;
    if let Some(output) = function.output.as_ref() {
        validate_return_against_signature(snapshot, type_manager, function.name.as_str(), &return_, output)?;
    }
    let first_match_annotations = stages
        .iter()
        .filter_map(|stage| {
            if let AnnotatedStage::Match { block_annotations, .. } = stage {
                Some(block_annotations)
            } else {
                None
            }
        })
        .next()
        .unwrap();
    let argument_annotations = arguments
        .iter()
        .map(|var| {
            if let Some(types_) = first_match_annotations.vertex_annotations_of(&Vertex::Variable(var.clone())) {
                let types_: &BTreeSet<Type> = &types_;
                FunctionParameterAnnotation::Concept(types_.clone())
            } else if let Some(ExpressionValueType::Single(value_type)) = argument_value_variable_types.get(var) {
                FunctionParameterAnnotation::Value(value_type.clone())
            } else {
                unreachable!()
            }
        })
        .collect();

    Ok(AnnotatedFunction {
        variable_registry: context.variable_registry.clone(),
        parameter_registry: parameters.clone(),
        arguments: arguments.clone(),
        stages,
        return_,
        argument_annotations,
    })
}

fn validate_return_against_signature(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
    annotated: &AnnotatedFunctionReturn,
    signature_return: &Output,
) -> Result<(), Box<FunctionAnnotationError>> {
    let return_labels = match signature_return {
        Output::Stream(stream) => &stream.types,
        Output::Single(single) => &single.types,
    };
    let declared_types = return_labels
        .iter()
        .enumerate()
        .map(|(i, label)| get_return_annotations_from_labels(snapshot, type_manager, label, i))
        .collect::<Result<Vec<_>, Box<FunctionAnnotationError>>>()?;
    let inferred_types: Vec<FunctionParameterAnnotation> = match annotated {
        AnnotatedFunctionReturn::Stream { annotations, .. } | AnnotatedFunctionReturn::Single { annotations, .. } => {
            annotations.iter().cloned().collect()
        }
        AnnotatedFunctionReturn::ReduceCheck { .. } => vec![],
        AnnotatedFunctionReturn::ReduceReducer { instructions } => {
            debug_assert!(instructions.len() == declared_types.len());
            instructions.iter().map(|reducer| FunctionParameterAnnotation::Value(reducer.output_type())).collect()
        }
    };
    debug_assert!(inferred_types.len() == declared_types.len());
    zip(inferred_types, declared_types).enumerate().try_for_each(|(i, (inferred, declared))| {
        let matches = match (&inferred, &declared) {
            (
                FunctionParameterAnnotation::Concept(inferred_types),
                FunctionParameterAnnotation::Concept(declared_types),
            ) => inferred_types.iter().all(|type_| declared_types.contains(type_)),
            (
                FunctionParameterAnnotation::Value(inferred_value),
                FunctionParameterAnnotation::Value(declared_value),
            ) => {
                declared_value == inferred_value
            }
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(Box::new(FunctionAnnotationError::SignatureReturnMismatch {
                function_name: name.to_owned(),
                mismatching_index: i,
            }))
        }
    })
}

fn annotate_signature_based_on_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    function: &Function,
) -> Result<AnnotatedFunctionSignature, Box<FunctionAnnotationError>> {
    let argument_annotations: Vec<FunctionParameterAnnotation> = function
        .argument_labels
        .as_ref()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, label)| get_argument_annotations_from_labels(snapshot, type_manager, label, index))
        .collect::<Result<_, Box<FunctionAnnotationError>>>()?;
    let returned = match function.output.as_ref().unwrap() {
        Output::Stream(stream) => stream.types.iter(),
        Output::Single(single) => single.types.iter(),
    }
    .enumerate()
    .map(|(index, label)| get_return_annotations_from_labels(snapshot, type_manager, label, index))
    .collect::<Result<_, Box<FunctionAnnotationError>>>()?;

    Ok(AnnotatedFunctionSignature { arguments: argument_annotations, returned })
}

fn get_argument_annotations_from_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    typeql_label: &TypeRefAny,
    arg_index: usize,
) -> Result<FunctionParameterAnnotation, Box<FunctionAnnotationError>> {
    get_annotations_from_labels(snapshot, type_manager, typeql_label)
        .map_err(|source| Box::new(FunctionAnnotationError::CouldNotResolveArgumentType { index: arg_index, source }))
}

fn get_return_annotations_from_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    typeql_label: &TypeRefAny,
    return_index: usize,
) -> Result<FunctionParameterAnnotation, Box<FunctionAnnotationError>> {
    get_annotations_from_labels(snapshot, type_manager, typeql_label)
        .map_err(|source| Box::new(FunctionAnnotationError::CouldNotResolveReturnType { index: return_index, source }))
}

fn get_annotations_from_labels(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    typeql_label: &TypeRefAny,
) -> Result<FunctionParameterAnnotation, TypeInferenceError> {
    let TypeRef::Named(inner_type) = (match typeql_label {
        TypeRefAny::Type(inner) => inner,
        TypeRefAny::Optional(typeql::type_::Optional { inner: _inner, .. }) => todo!(),
        TypeRefAny::List(typeql::type_::List { inner: _inner, .. }) => todo!(),
    }) else {
        unreachable!("Function return labels cannot be variable.");
    };
    match inner_type {
        NamedType::Label(label) => {
            // TODO: could be a struct value type in the future!
            let types = type_seeder::get_type_annotation_and_subtypes_from_label(
                snapshot,
                type_manager,
                &Label::build(label.ident.as_str()),
            )?;
            Ok(FunctionParameterAnnotation::Concept(types))
        }
        NamedType::BuiltinValueType(value_type) => {
            // TODO: This may be list
            let value = translate_value_type(&value_type.token);
            Ok(FunctionParameterAnnotation::Value(value))
        }
        NamedType::Role(_) => unreachable!("A function return label was wrongly parsed as role-type."),
    }
}

fn annotate_return(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    variable_registry: &VariableRegistry,
    return_operation: &ReturnOperation,
    input_type_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    input_value_type_annotations: &BTreeMap<Variable, ExpressionValueType>,
) -> Result<AnnotatedFunctionReturn, Box<FunctionAnnotationError>> {
    match return_operation {
        ReturnOperation::Stream(vars) => {
            let type_annotations = vars
                .iter()
                .map(|var| get_function_parameter(var, input_type_annotations, input_value_type_annotations))
                .collect();
            Ok(AnnotatedFunctionReturn::Stream { variables: vars.clone(), annotations: type_annotations })
        }
        ReturnOperation::Single(selector, vars) => {
            let type_annotations = vars
                .iter()
                .map(|var| get_function_parameter(var, input_type_annotations, input_value_type_annotations))
                .collect();
            Ok(AnnotatedFunctionReturn::Single {
                selector: selector.clone(),
                variables: vars.clone(),
                annotations: type_annotations,
            })
        }
        ReturnOperation::ReduceReducer(reducers) => {
            let mut instructions = Vec::with_capacity(reducers.len());
            for &reducer in reducers {
                let instruction = resolve_reducer_by_value_type(
                    snapshot,
                    type_manager,
                    variable_registry,
                    reducer,
                    input_type_annotations,
                    input_value_type_annotations,
                )
                .map_err(|err| Box::new(FunctionAnnotationError::ReturnReduce { typedb_source: Box::new(err) }))?;
                instructions.push(instruction);
            }
            Ok(AnnotatedFunctionReturn::ReduceReducer { instructions })
        }
        ReturnOperation::ReduceCheck() => Ok(AnnotatedFunctionReturn::ReduceCheck {}),
    }
}

fn get_function_parameter(
    variable: &Variable,
    body_variable_annotations: &BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    body_variable_value_types: &BTreeMap<Variable, ExpressionValueType>,
) -> FunctionParameterAnnotation {
    if let Some(arced_types) = body_variable_annotations.get(variable) {
        let types: &BTreeSet<Type> = arced_types;
        FunctionParameterAnnotation::Concept(types.clone())
    } else if let Some(expression_value_type) = body_variable_value_types.get(variable) {
        FunctionParameterAnnotation::Value(expression_value_type.value_type().clone())
    } else {
        unreachable!("Could not find annotations for a function return variable.")
    }
}
