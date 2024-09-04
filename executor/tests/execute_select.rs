/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use compiler::{
    match_::{
        inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
        instructions::{ConstraintInstruction, HasInstruction, Inputs},
        planner::{
            pattern_plan::{IntersectionProgram, MatchProgram, Program},
            program_plan::ProgramPlan,
        },
    },
    VariablePosition,
};
use concept::{
    error::ConceptReadError,
    thing::object::ObjectAPI,
    type_::{annotation::AnnotationCardinality, owns::OwnsAnnotation, OwnerAPI},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{program_executor::ProgramExecutor, row::MaybeOwnedRow};
use ir::{pattern::constraint::IsaKind, program::block::FunctionalBlock, translation::TranslationContext};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot},
    MVCCStorage,
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");
const EMAIL_LABEL: Label = Label::new_static("email");

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone());
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, &PERSON_LABEL).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    let person_owns_age = person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone()).unwrap();
    person_owns_age
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, Some(10))),
        )
        .unwrap();
    let person_owns_name =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone()).unwrap();
    person_owns_name
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, Some(10))),
        )
        .unwrap();
    let email_type = type_manager.create_attribute_type(&mut snapshot, &EMAIL_LABEL).unwrap();
    email_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    let person_owns_email =
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, email_type.clone()).unwrap();
    person_owns_email
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, Some(10))),
        )
        .unwrap();

    let _person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let _person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let _person_3 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();

    let mut _age_1 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap();
    let mut _age_2 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap();
    let mut _age_3 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap();
    let mut _age_4 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap();
    let mut _age_5 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap();

    let mut _name_1 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Abby".to_string())))
        .unwrap();
    let mut _name_2 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Bobby".to_string())))
        .unwrap();
    let mut _name_3 = thing_manager
        .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned("Candice".to_string())))
        .unwrap();

    let mut _email_1 = thing_manager
        .create_attribute(&mut snapshot, email_type.clone(), Value::String(Cow::Owned("abc@email.com".to_string())))
        .unwrap();
    let mut _email_2 = thing_manager
        .create_attribute(&mut snapshot, email_type.clone(), Value::String(Cow::Owned("xyz@email.com".to_string())))
        .unwrap();

    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_1.clone()).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_2.clone()).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _age_3.clone()).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _name_1.clone()).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _name_2.clone()).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _email_1.clone()).unwrap();
    _person_1.set_has_unordered(&mut snapshot, &thing_manager, _email_2.clone()).unwrap();

    _person_2.set_has_unordered(&mut snapshot, &thing_manager, _age_5.clone()).unwrap();
    _person_2.set_has_unordered(&mut snapshot, &thing_manager, _age_4.clone()).unwrap();
    _person_2.set_has_unordered(&mut snapshot, &thing_manager, _age_1.clone()).unwrap();

    _person_3.set_has_unordered(&mut snapshot, &thing_manager, _age_4.clone()).unwrap();
    _person_3.set_has_unordered(&mut snapshot, &thing_manager, _name_3.clone()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
}

#[test]
fn anonymous_vars_not_enumerated_or_counted() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has $_;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_attribute_type = conjunction.declare_variable_anonymous().unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_attribute = conjunction.declare_variable_anonymous().unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let (entry_annotations, _) = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(
            &entry,
            vec![],
            &snapshot,
            &type_manager,
            &IndexedAnnotatedFunctions::empty(),
            &translation_context.variable_registry,
        )
        .unwrap()
    };

    let vars = vec![var_attribute, var_person, var_attribute_type, var_person_type];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));

    // Plan
    let steps = vec![Program::Intersection(IntersectionProgram::new(
        variable_positions[&var_person],
        vec![ConstraintInstruction::Has(
            HasInstruction::new(has_attribute, Inputs::None([]), &entry_annotations).map(&variable_positions),
        )],
        &[variable_positions[&var_person]],
    ))];
    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let (_, thing_manager) = load_managers(storage.clone());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);
    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();

    // person1, <something>
    // person2, <something>
    // person3, <something>

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_multiplicity(), 1);
    assert_eq!(rows[1].as_ref().unwrap().get_multiplicity(), 1);
    assert_eq!(rows[2].as_ref().unwrap().get_multiplicity(), 1);

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        print!("{}", r);
    }
}

#[test]
fn unselected_named_vars_counted() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has $attr;
    //   select $person;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_attribute_type = conjunction.get_or_declare_variable("attr_type").unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_attribute = conjunction.get_or_declare_variable("attr").unwrap();
    let has_attribute = conjunction.constraints_mut().add_has(var_person, var_attribute).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let (entry_annotations, _) = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(
            &entry,
            vec![],
            &snapshot,
            &type_manager,
            &IndexedAnnotatedFunctions::empty(),
            &translation_context.variable_registry,
        )
        .unwrap()
    };

    let vars = vec![var_person_type, var_attribute, var_attribute_type, var_person];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));

    // Plan
    let steps = vec![Program::Intersection(IntersectionProgram::new(
        variable_positions[&var_person],
        vec![ConstraintInstruction::Has(
            HasInstruction::new(has_attribute, Inputs::None([]), &entry_annotations).map(&variable_positions),
        )],
        &[variable_positions[&var_person]],
    ))];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
    let (_, thing_manager) = load_managers(storage.clone());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);
    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();

    // 7x person 1, <something>
    // 3x person 2, <something>
    // 2x person 3, <something>

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_multiplicity(), 7);
    assert_eq!(rows[1].as_ref().unwrap().get_multiplicity(), 3);
    assert_eq!(rows[2].as_ref().unwrap().get_multiplicity(), 2);

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        print!("{}", r);
    }
}

#[test]
fn cartesian_named_counted_checked() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //   match
    //    $person has name $name, has age $age, has email $_;
    //   select $person, $name;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_name_type = conjunction.declare_variable_anonymous().unwrap();
    let var_age_type = conjunction.declare_variable_anonymous().unwrap();
    let var_email_type = conjunction.declare_variable_anonymous().unwrap();
    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_name = conjunction.get_or_declare_variable("name").unwrap();
    let var_age = conjunction.get_or_declare_variable("age").unwrap();
    let var_email = conjunction.declare_variable_anonymous().unwrap();
    let has_name = conjunction.constraints_mut().add_has(var_person, var_name).unwrap().clone();
    let has_age = conjunction.constraints_mut().add_has(var_person, var_age).unwrap().clone();
    let has_email = conjunction.constraints_mut().add_has(var_person, var_email).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_email, var_email_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_name_type, NAME_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_email_type, EMAIL_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let (entry_annotations, _) = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(
            &entry,
            vec![],
            &snapshot,
            &type_manager,
            &IndexedAnnotatedFunctions::empty(),
            &translation_context.variable_registry,
        )
        .unwrap()
    };

    let vars =
        vec![var_age_type, var_person_type, var_name_type, var_email_type, var_name, var_email, var_person, var_age];
    let variable_positions =
        HashMap::from_iter(vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));

    // Plan
    let steps = vec![Program::Intersection(IntersectionProgram::new(
        variable_positions[&var_person],
        vec![
            ConstraintInstruction::Has(
                HasInstruction::new(has_name, Inputs::None([]), &entry_annotations).map(&variable_positions),
            ),
            ConstraintInstruction::Has(
                HasInstruction::new(has_age, Inputs::None([]), &entry_annotations).map(&variable_positions),
            ),
            ConstraintInstruction::Has(
                HasInstruction::new(has_email, Inputs::None([]), &entry_annotations).map(&variable_positions),
            ),
        ],
        &[variable_positions[&var_person], variable_positions[&var_age]],
    ))];

    let pattern_plan =
        MatchProgram::new(steps, translation_context.variable_registry.clone(), variable_positions, vars);
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
    let (_, thing_manager) = load_managers(storage.clone());
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);
    let rows: Vec<Result<MaybeOwnedRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.as_reference().into_owned()).map_err(|err| err.clone())).collect();

    // 2x person 1, age_1, <name something>, <email something>
    // 2x person 1, age_2, <name something>, <email something>
    // 2x person 1, age_3, <name something>, <email something>

    for row in rows.iter() {
        let r = row.as_ref().unwrap();
        print!("{}", r);
    }

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].as_ref().unwrap().get_multiplicity(), 2);
    assert_eq!(rows[1].as_ref().unwrap().get_multiplicity(), 2);
    assert_eq!(rows[2].as_ref().unwrap().get_multiplicity(), 2);
}