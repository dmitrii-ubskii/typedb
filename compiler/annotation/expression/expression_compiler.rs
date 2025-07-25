/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::value::{
    value_type::{ValueType, ValueTypeCategory},
    ValueEncodable,
};
use ir::{
    pattern::{
        expression::{
            BuiltInCall, BuiltInFunctionID, Expression, ExpressionTree, ListConstructor, ListIndex, ListIndexRange,
            Operation, Operator,
        },
        ParameterID,
    },
    pipeline::ParameterRegistry,
};
use typeql::common::Span;

use crate::annotation::expression::{
    compiled_expression::{ExecutableExpression, ExpressionValueType},
    instructions::{
        list_operations,
        load_cast::{
            CastLeftDecimalToDouble, CastLeftIntegerToDecimal, CastLeftIntegerToDouble, CastRightDecimalToDouble,
            CastRightIntegerToDecimal, CastRightIntegerToDouble, LoadConstant, LoadVariable,
        },
        op_codes::ExpressionOpCode,
        operators,
        unary::{MathAbsDouble, MathAbsInteger, MathCeilDouble, MathFloorDouble, MathRoundDouble},
        CompilableExpression, ExpressionInstruction,
    },
    ExpressionCompileError,
};

pub struct ExpressionCompilationContext<'this> {
    expression_tree: &'this ExpressionTree<Variable>,
    variable_value_categories: &'this HashMap<Variable, ExpressionValueType>,
    parameters: &'this ParameterRegistry,
    type_stack: Vec<ExpressionValueType>,

    instructions: Vec<ExpressionOpCode>,
    variable_stack: Vec<Variable>,
    constant_stack: Vec<ParameterID>,
}

impl<'this> ExpressionCompilationContext<'this> {
    fn empty(
        expression_tree: &'this ExpressionTree<Variable>,
        variable_value_categories: &'this HashMap<Variable, ExpressionValueType>,
        parameters: &'this ParameterRegistry,
    ) -> Self {
        ExpressionCompilationContext {
            expression_tree,
            variable_value_categories,
            parameters,
            instructions: Vec::new(),
            variable_stack: Vec::new(),
            constant_stack: Vec::new(),
            type_stack: Vec::new(),
        }
    }

    pub fn compile(
        expression_tree: &ExpressionTree<Variable>,
        variable_value_categories: &HashMap<Variable, ExpressionValueType>,
        parameters: &ParameterRegistry,
    ) -> Result<ExecutableExpression<Variable>, Box<ExpressionCompileError>> {
        debug_assert!(expression_tree.argument_ids().all(|var| variable_value_categories.contains_key(&var)));
        let mut builder = ExpressionCompilationContext::empty(expression_tree, variable_value_categories, parameters);
        builder.compile_recursive(expression_tree.get_root())?;
        let return_type = builder.pop_type()?;
        let ExpressionCompilationContext { instructions, variable_stack, constant_stack, .. } = builder;
        Ok(ExecutableExpression { instructions, variables: variable_stack, constants: constant_stack, return_type })
    }

    fn compile_recursive(&mut self, expression: &Expression<Variable>) -> Result<(), Box<ExpressionCompileError>> {
        match expression {
            Expression::Constant(constant) => self.compile_constant(*constant),
            Expression::Variable(variable) => self.compile_variable(variable),
            Expression::Operation(op) => self.compile_op(op),
            Expression::BuiltInCall(builtin) => self.compile_builtin(builtin),
            Expression::ListIndex(list_index) => self.compile_list_index(list_index),
            Expression::List(list_constructor) => self.compile_list_constructor(list_constructor),
            Expression::ListIndexRange(list_index_range) => self.compile_list_index_range(list_index_range),
        }
    }

    fn compile_constant(&mut self, constant: ParameterID) -> Result<(), Box<ExpressionCompileError>> {
        self.constant_stack.push(constant);

        self.push_type_single(self.parameters.value_unchecked(constant).value_type());
        self.append_instruction(LoadConstant::OP_CODE);

        Ok(())
    }

    fn compile_variable(&mut self, variable: &Variable) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(self.variable_value_categories.contains_key(variable));

        self.variable_stack.push(*variable);
        self.append_instruction(LoadVariable::OP_CODE);
        // TODO: We need a way to know if a variable is a list or a single
        match self.variable_value_categories.get(variable).unwrap() {
            ExpressionValueType::Single(value_type) => self.push_type_single(value_type.clone()),
            ExpressionValueType::List(value_type) => self.push_type_list(value_type.clone()),
        }
        Ok(())
    }

    fn compile_list_constructor(
        &mut self,
        list_constructor: &ListConstructor,
    ) -> Result<(), Box<ExpressionCompileError>> {
        for expression_id in list_constructor.item_expression_ids().iter().rev() {
            self.compile_recursive(self.expression_tree.get(*expression_id))?;
        }

        self.compile_constant(list_constructor.len_id())?;
        self.append_instruction(list_operations::ListConstructor::OP_CODE);

        if self.pop_type_single()?.category() != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::InternalListLengthMustBeInteger {})?;
        }
        let n_elements = list_constructor.item_expression_ids().len();
        if n_elements > 0 {
            let element_type = self.pop_type_single()?;
            for _ in 1..list_constructor.item_expression_ids().len() {
                if self.pop_type_single()? != element_type {
                    Err(ExpressionCompileError::HeterogeneusListConstructor {
                        source_span: list_constructor.source_span(),
                    })?;
                }
            }
            self.push_type_list(element_type)
        } else {
            Err(ExpressionCompileError::EmptyListConstructorCannotInferValueType {
                source_span: list_constructor.source_span(),
            })?;
        }

        Ok(())
    }

    fn compile_list_index(&mut self, list_index: &ListIndex<Variable>) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index.list_variable()));

        self.compile_recursive(self.expression_tree.get(list_index.index_expression_id()))?;
        self.compile_variable(&list_index.list_variable())?;

        self.append_instruction(list_operations::ListIndex::OP_CODE);

        let list_variable_type = self.pop_type_list()?;
        let index_type = self.pop_type_single()?.category();
        if index_type != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::ListIndexMustBeInteger { source_span: list_index.source_span() })?
        }
        self.push_type_single(list_variable_type); // reuse
        Ok(())
    }

    fn compile_list_index_range(
        &mut self,
        list_index_range: &ListIndexRange<Variable>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index_range.list_variable()));
        self.compile_recursive(self.expression_tree.get(list_index_range.from_expression_id()))?;
        self.compile_recursive(self.expression_tree.get(list_index_range.to_expression_id()))?;
        self.compile_variable(&list_index_range.list_variable())?;

        self.append_instruction(list_operations::ListIndexRange::OP_CODE);

        let list_variable_type = self.pop_type_list()?;
        let from_index_type = self.pop_type_single()?.category();
        if from_index_type != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::ListIndexMustBeInteger { source_span: list_index_range.source_span() })?
        }
        let to_index_type = self.pop_type_single()?.category();
        if to_index_type != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::ListIndexMustBeInteger { source_span: list_index_range.source_span() })?
        }

        self.push_type_single(list_variable_type);
        Ok(())
    }

    fn compile_op(&mut self, operation: &Operation) -> Result<(), Box<ExpressionCompileError>> {
        let operator = operation.operator();
        let right_expression = self.expression_tree.get(operation.right_expression_id());
        self.compile_recursive(self.expression_tree.get(operation.left_expression_id()))?;
        let left_category = self.peek_type_single()?.category();
        match left_category {
            ValueTypeCategory::Boolean => self.compile_op_boolean(operator, right_expression, operation.source_span()),
            ValueTypeCategory::Integer => self.compile_op_integer(operator, right_expression, operation.source_span()),
            ValueTypeCategory::Double => self.compile_op_double(operator, right_expression, operation.source_span()),
            ValueTypeCategory::Decimal => self.compile_op_decimal(operator, right_expression, operation.source_span()),
            ValueTypeCategory::Date => self.compile_op_date(operator, right_expression, operation.source_span()),
            ValueTypeCategory::DateTime => {
                self.compile_op_datetime(operator, right_expression, operation.source_span())
            }
            ValueTypeCategory::DateTimeTZ => {
                self.compile_op_datetime_tz(operator, right_expression, operation.source_span())
            }
            ValueTypeCategory::Duration => {
                self.compile_op_duration(operator, right_expression, operation.source_span())
            }
            ValueTypeCategory::String => self.compile_op_string(operator, right_expression, operation.source_span()),
            ValueTypeCategory::Struct => self.compile_op_struct(operator, right_expression, operation.source_span()),
        }
    }

    fn compile_op_boolean(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Boolean,
            right_category,
            source_span,
        }))
    }

    fn compile_op_integer(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        match right_category {
            ValueTypeCategory::Integer => {
                self.compile_op_integer_integer(op)?;
            }
            ValueTypeCategory::Double => {
                CastLeftIntegerToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            ValueTypeCategory::Decimal => match op {
                Operator::Add => {
                    CastLeftIntegerToDecimal::validate_and_append(self)?;
                    operators::OpDecimalAddDecimal::validate_and_append(self)?;
                }
                Operator::Subtract => {
                    CastLeftIntegerToDecimal::validate_and_append(self)?;
                    operators::OpDecimalSubtractDecimal::validate_and_append(self)?;
                }
                Operator::Multiply => {
                    CastLeftIntegerToDecimal::validate_and_append(self)?;
                    operators::OpDecimalMultiplyDecimal::validate_and_append(self)?;
                }
                other_op => {
                    CastLeftIntegerToDouble::validate_and_append(self)?;
                    CastRightDecimalToDouble::validate_and_append(self)?;
                    self.compile_op_double_double(other_op)?;
                }
            },
            _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Integer,
                right_category,
                source_span,
            })?,
        }
        Ok(())
    }

    fn compile_op_double(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        match right_category {
            ValueTypeCategory::Integer => {
                // The right needs to be cast
                CastRightIntegerToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            ValueTypeCategory::Decimal => {
                // The right needs to be cast
                CastRightDecimalToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            ValueTypeCategory::Double => {
                self.compile_op_double_double(op)?;
            }
            _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Double,
                right_category,
                source_span,
            })?,
        }
        Ok(())
    }

    fn compile_op_decimal(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        match right_category {
            ValueTypeCategory::Integer => match op {
                Operator::Add => {
                    CastRightIntegerToDecimal::validate_and_append(self)?;
                    operators::OpDecimalAddDecimal::validate_and_append(self)?;
                }
                Operator::Subtract => {
                    CastRightIntegerToDecimal::validate_and_append(self)?;
                    operators::OpDecimalSubtractDecimal::validate_and_append(self)?;
                }
                Operator::Multiply => {
                    CastRightIntegerToDecimal::validate_and_append(self)?;
                    operators::OpDecimalMultiplyDecimal::validate_and_append(self)?;
                }
                other_op => {
                    CastLeftDecimalToDouble::validate_and_append(self)?;
                    CastRightIntegerToDouble::validate_and_append(self)?;
                    self.compile_op_double_double(other_op)?;
                }
            },
            ValueTypeCategory::Double => {
                CastLeftDecimalToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            ValueTypeCategory::Decimal => match op {
                Operator::Add => operators::OpDecimalAddDecimal::validate_and_append(self)?,
                Operator::Subtract => operators::OpDecimalSubtractDecimal::validate_and_append(self)?,
                Operator::Multiply => operators::OpDecimalMultiplyDecimal::validate_and_append(self)?,
                other_op => {
                    CastLeftDecimalToDouble::validate_and_append(self)?;
                    CastRightDecimalToDouble::validate_and_append(self)?;
                    self.compile_op_double_double(other_op)?;
                }
            },
            _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Decimal,
                right_category,
                source_span,
            })?,
        }
        Ok(())
    }

    fn compile_op_string(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::String,
            right_category,
            source_span,
        }))
    }

    fn compile_op_date(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Date,
            right_category,
            source_span,
        }))
    }

    fn compile_op_datetime(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::DateTime,
            right_category,
            source_span,
        }))
    }

    fn compile_op_datetime_tz(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::DateTimeTZ,
            right_category,
            source_span,
        }))
    }

    fn compile_op_duration(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Duration,
            right_category,
            source_span,
        }))
    }

    fn compile_op_struct(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
        source_span: Option<Span>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.category();
        Err(Box::new(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Struct,
            right_category,
            source_span,
        }))
    }

    // Ops with Left, Right resolved
    fn compile_op_integer_integer(&mut self, op: Operator) -> Result<(), Box<ExpressionCompileError>> {
        match op {
            Operator::Add => operators::OpIntegerAddInteger::validate_and_append(self)?,
            Operator::Subtract => operators::OpIntegerSubtractInteger::validate_and_append(self)?,
            Operator::Multiply => operators::OpIntegerMultiplyInteger::validate_and_append(self)?,
            Operator::Divide => operators::OpIntegerDivideInteger::validate_and_append(self)?,
            Operator::Modulo => operators::OpIntegerModuloInteger::validate_and_append(self)?,
            Operator::Power => operators::OpIntegerPowerInteger::validate_and_append(self)?,
        }
        Ok(())
    }

    fn compile_op_double_double(&mut self, op: Operator) -> Result<(), Box<ExpressionCompileError>> {
        match op {
            Operator::Add => operators::OpDoubleAddDouble::validate_and_append(self)?,
            Operator::Subtract => operators::OpDoubleSubtractDouble::validate_and_append(self)?,
            Operator::Multiply => operators::OpDoubleMultiplyDouble::validate_and_append(self)?,
            Operator::Divide => operators::OpDoubleDivideDouble::validate_and_append(self)?,
            Operator::Modulo => operators::OpDoubleModuloDouble::validate_and_append(self)?,
            Operator::Power => operators::OpDoublePowerDouble::validate_and_append(self)?,
        }
        Ok(())
    }

    fn compile_builtin(&mut self, builtin: &BuiltInCall) -> Result<(), Box<ExpressionCompileError>> {
        match builtin.builtin_id() {
            BuiltInFunctionID::Abs => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Integer => MathAbsInteger::validate_and_append(self)?,
                    ValueTypeCategory::Double => MathAbsDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.builtin_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltInFunctionID::Ceil => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Double => MathCeilDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.builtin_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltInFunctionID::Floor => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Double => MathFloorDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.builtin_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltInFunctionID::Round => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Double => MathRoundDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.builtin_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
        }
        Ok(())
    }

    fn pop_type(&mut self) -> Result<ExpressionValueType, Box<ExpressionCompileError>> {
        match self.type_stack.pop() {
            Some(value) => Ok(value),
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn pop_type_single(&mut self) -> Result<ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.pop() {
            Some(ExpressionValueType::Single(value)) => Ok(value),
            Some(ExpressionValueType::List(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedSingleWasList {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn pop_type_list(&mut self) -> Result<ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.pop() {
            Some(ExpressionValueType::List(value)) => Ok(value),
            Some(ExpressionValueType::Single(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedListWasSingle {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn push_type_single(&mut self, value: ValueType) {
        self.type_stack.push(ExpressionValueType::Single(value));
    }

    pub(crate) fn push_type_list(&mut self, value: ValueType) {
        self.type_stack.push(ExpressionValueType::List(value));
    }

    fn peek_type_single(&self) -> Result<&ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.last() {
            Some(ExpressionValueType::Single(value)) => Ok(value),
            Some(ExpressionValueType::List(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedSingleWasList {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn peek_type_list(&mut self) -> Result<&ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.last() {
            Some(ExpressionValueType::List(value)) => Ok(value),
            Some(ExpressionValueType::Single(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedListWasSingle {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn append_instruction(&mut self, op_code: ExpressionOpCode) {
        self.instructions.push(op_code)
    }
}
