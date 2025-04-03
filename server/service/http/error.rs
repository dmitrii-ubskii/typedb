/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use database::{database::DatabaseCreateError, DatabaseDeleteError};
use error::{typedb_error, TypeDBError};
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
        // TODO: Should be reversed? Currently, it's like:
        /*
        {
            "code": "HSR6",
            "message": "[AUT1] Invalid credential supplied.\n[HSR6] Authentication error."
        }
         */
        self.stack_trace().join("\n").to_string()
    }

    pub(crate) fn operation_not_permitted() -> Self {
        Self::Service { typedb_source: ServiceError::OperationNotPermitted {} }
    }

    pub(crate) fn no_open_transaction() -> Self {
        Self::Transaction { typedb_source: TransactionServiceError::NoOpenTransaction {} }
    }
}
