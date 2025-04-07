/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, str::FromStr};

use axum::{
    extract::{FromRequest, FromRequestParts, Path},
    response::{IntoResponse, Response},
    RequestExt, RequestPartsExt,
};
use futures::TryFutureExt;
use http::StatusCode;
use options::TransactionOptions;
use resource::constants::server::{
    DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS, DEFAULT_TRANSACTION_PARALLEL, DEFAULT_TRANSACTION_TIMEOUT_MILLIS,
};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::service::{
    http::{
        error::HTTPServiceError, message::from_request_parts_impl, transaction_service::TransactionServiceResponse,
    },
    TransactionType,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TransactionOpenPayload {
    pub(crate) database_name: String,
    pub(crate) transaction_type: TransactionType,
    pub(crate) options: Option<TransactionOptionsPayload>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TransactionOptionsPayload {
    pub parallel: Option<bool>,
    pub schema_lock_acquire_timeout_millis: Option<u64>,
    pub transaction_timeout_millis: Option<u64>,
}

impl Into<TransactionOptions> for TransactionOptionsPayload {
    fn into(self) -> TransactionOptions {
        TransactionOptions {
            parallel: self.parallel.unwrap_or(DEFAULT_TRANSACTION_PARALLEL),
            schema_lock_acquire_timeout_millis: self
                .schema_lock_acquire_timeout_millis
                .unwrap_or(DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS),
            transaction_timeout_millis: self.transaction_timeout_millis.unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_MILLIS),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionResponse {
    pub transaction_id: Uuid,
}

pub(crate) fn encode_transaction(transaction_id: Uuid) -> TransactionResponse {
    TransactionResponse { transaction_id }
}

#[derive(Debug)]
pub(crate) struct TransactionPath {
    pub(crate) transaction_id: Uuid,
}

from_request_parts_impl!(TransactionPath { transaction_id: Uuid });

impl IntoResponse for TransactionServiceResponse {
    fn into_response(self) -> Response {
        match self {
            TransactionServiceResponse::Ok => StatusCode::OK.into_response(),
            TransactionServiceResponse::Query(query) => query.into_response(),
            TransactionServiceResponse::Err(typedb_source) => {
                HTTPServiceError::Transaction { typedb_source }.into_response()
            }
        }
    }
}
