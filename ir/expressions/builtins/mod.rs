/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::expressions::ExpressionEvaluationError;

pub(crate) mod binary;
pub(crate) mod list_operations;
pub(crate) mod load_cast;
pub(crate) mod operators;
pub(crate) mod unary;

fn check_operation<T>(checked_operation_result: Option<T>) -> Result<T, ExpressionEvaluationError> {
    match checked_operation_result {
        None => Err(ExpressionEvaluationError::CheckedOperationFailed),
        Some(result) => Ok(result),
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum BuiltInFunctionID {
    Abs,
    Ceil,
    Floor,
    Round,
    // TODO: The below
    // Max,
    // Min,
    // Length
}
