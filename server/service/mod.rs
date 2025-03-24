/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use error::typedb_error;

pub(crate) mod control_flow;
pub(crate) mod grpc;
pub(crate) mod http;
mod transaction_service;

typedb_error! {
    ServiceError(component = "Server", prefix = "SRV") {
        Unimplemented(1, "Not implemented: {description}", description: String),
        OperationNotPermitted(2, "The user is not permitted to execute the operation"),
        DatabaseDoesNotExist(3, "Database '{name}' does not exist.", name: String),
        UserDoesNotExist(4, "User does not exist"),
    }
}
