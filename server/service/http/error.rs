/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use axum::response::{IntoResponse, Response};
use database::{database::DatabaseCreateError, DatabaseDeleteError};
use error::{typedb_error, TypeDBError};
use http::StatusCode;
use user::errors::{UserCreateError, UserDeleteError, UserGetError, UserUpdateError};

use crate::{
    authentication::AuthenticationError,
    service::{transaction_service::TransactionServiceError, ServiceError},
};

typedb_error!(
    pub HTTPServiceError(component = "HTTP Service", prefix = "HSR") {
        Internal(1, "Internal error: {details}", details: String),
        JsonBodyExpected(2, "Cannot parse expected JSON body: {details}", details: String),
        RequestTimeout(3, "Request timeout."),
        NotFound(4, "Requested resource not found."),
        Service(5, "Service error.", typedb_source: ServiceError),
        Authentication(6, "Authentication error.", typedb_source: AuthenticationError),
        DatabaseCreate(7, "Database create error.", typedb_source: DatabaseCreateError),
        DatabaseDelete(8, "Database delete error.", typedb_source: DatabaseDeleteError),
        UserCreate(9, "User create error.", typedb_source: UserCreateError),
        UserUpdate(10, "User update error.", typedb_source: UserUpdateError),
        UserDelete(11, "User delete error.", typedb_source: UserDeleteError),
        UserGet(12, "User get error.", typedb_source: UserGetError),
        Transaction(13, "Transaction error.", typedb_source: TransactionServiceError),
        QueryClose(14, "Error while closing single-query transaction.", typedb_source: TransactionServiceError),
        QueryCommit(15, "Error while committing single-query transaction.", typedb_source: TransactionServiceError),
    }
);

impl HTTPServiceError {
    pub(crate) fn source(&self) -> &(dyn TypeDBError + Sync + '_) {
        self.source_typedb_error().unwrap_or(self)
    }

    pub(crate) fn format_source_trace(&self) -> String {
        self.stack_trace().join("\n").to_string()
    }

    pub(crate) fn operation_not_permitted() -> Self {
        Self::Service { typedb_source: ServiceError::OperationNotPermitted {} }
    }

    pub(crate) fn no_open_transaction() -> Self {
        Self::Transaction { typedb_source: TransactionServiceError::NoOpenTransaction {} }
    }
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
        (code, self.format_source_trace()).into_response()
    }
}
