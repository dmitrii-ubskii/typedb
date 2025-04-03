/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use axum::response::{IntoResponse, Response};
use error::TypeDBError;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::service::{
    http::{error::HTTPServiceError, message::body::JsonBody},
    ServiceError,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

impl IntoResponse for HTTPServiceError {
    fn into_response(self) -> Response {
        let code = match &self {
            HTTPServiceError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            HTTPServiceError::JsonBodyExpected { .. } => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            HTTPServiceError::RequestTimeout { .. } => StatusCode::REQUEST_TIMEOUT,
            HTTPServiceError::NotFound { .. } => StatusCode::NOT_FOUND,
            HTTPServiceError::Service { typedb_source } => match typedb_source {
                ServiceError::Unimplemented { .. } => StatusCode::NOT_IMPLEMENTED,
                ServiceError::OperationNotPermitted { .. } => StatusCode::FORBIDDEN,
                ServiceError::DatabaseDoesNotExist { .. } => StatusCode::NOT_FOUND,
                ServiceError::UserDoesNotExist { .. } => StatusCode::NOT_FOUND,
            },
            HTTPServiceError::Authentication { .. } => StatusCode::UNAUTHORIZED,
            HTTPServiceError::DatabaseCreate { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::DatabaseDelete { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::UserCreate { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::UserUpdate { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::UserDelete { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::UserGet { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::Transaction { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::QueryClose { .. } => StatusCode::BAD_REQUEST,
            HTTPServiceError::QueryCommit { .. } => StatusCode::BAD_REQUEST,
        };
        (code, JsonBody(encode_error(self))).into_response()
    }
}

pub(crate) fn encode_error(error: HTTPServiceError) -> ErrorResponse {
    ErrorResponse { code: error.code().to_string(), message: error.format_source_trace() }
}
