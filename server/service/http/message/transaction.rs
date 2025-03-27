/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, str::FromStr};

use axum::{
    async_trait,
    extract::{FromRequest, FromRequestParts, Path, Request},
    response::{IntoResponse, Response},
    Json, RequestExt, RequestPartsExt,
};
use futures::TryFutureExt;
use http::{header::CONTENT_TYPE, request::Parts, StatusCode};
use itertools::Itertools;
use options::TransactionOptions;
use resource::constants::server::{
    DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS, DEFAULT_TRANSACTION_PARALLEL, DEFAULT_TRANSACTION_TIMEOUT_MILLIS,
};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::service::{
    http::message::{
        database::{encode_database, DatabaseResponse},
        from_request_parts_impl,
    },
    TransactionType,
};

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TransactionId(pub(crate) Uuid);

impl FromStr for TransactionId {
    type Err = uuid::Error;

    fn from_str(uuid: &str) -> Result<Self, Self::Err> {
        Ok(TransactionId(Uuid::from_str(uuid)?))
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for TransactionId
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let params: Path<HashMap<String, String>> = parts.extract().await.map_err(IntoResponse::into_response)?;
        let id = params
            .get("transaction_id")
            .ok_or_else(|| (StatusCode::NOT_FOUND, "transaction_id param missing").into_response())?;
        match TransactionId::from_str(id.as_str()) {
            Ok(id) => Ok(id),
            Err(err) => Err((StatusCode::BAD_REQUEST, "transaction_id param is ill-formed").into_response()),
        }
    }
}

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

pub(crate) struct TransactionPath {
    pub(crate) transaction_id: TransactionId,
}

from_request_parts_impl!(TransactionPath { transaction_id: TransactionId });
