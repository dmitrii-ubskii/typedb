/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use itertools::Itertools;
use options::{QueryOptions, TransactionOptions};
use resource::constants::server::{
    DEFAULT_INCLUDE_INSTANCE_TYPES, DEFAULT_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS, DEFAULT_TRANSACTION_PARALLEL,
    DEFAULT_TRANSACTION_TIMEOUT_MILLIS,
};
use serde::{Deserialize, Serialize};

use crate::service::{http::message::transaction::TransactionOpenPayload, AnswerType, QueryType};

pub mod concept;
pub mod document;
pub mod row;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueryOptionsPayload {
    pub include_instance_types: Option<bool>,
}

impl Into<QueryOptions> for QueryOptionsPayload {
    fn into(self) -> QueryOptions {
        QueryOptions { include_instance_types: self.include_instance_types.unwrap_or(DEFAULT_INCLUDE_INSTANCE_TYPES) }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransactionQueryPayload {
    pub query_options: Option<QueryOptionsPayload>,
    pub query: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryPayload {
    pub query_options: Option<QueryOptionsPayload>,
    pub query: String,
    pub commit: Option<bool>,

    #[serde(flatten)]
    pub transaction_open_payload: TransactionOpenPayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryAnswerResponse {
    pub query_type: QueryType,
    pub answer_type: AnswerType,
    pub answers: Option<Vec<serde_json::Value>>,
}

pub(crate) fn encode_query_ok_answer(query_type: QueryType) -> QueryAnswerResponse {
    QueryAnswerResponse { query_type, answer_type: AnswerType::Ok, answers: None }
}

pub(crate) fn encode_query_rows_answer(query_type: QueryType, rows: Vec<serde_json::Value>) -> QueryAnswerResponse {
    QueryAnswerResponse { query_type, answer_type: AnswerType::ConceptRows, answers: Some(rows) }
}

pub(crate) fn encode_query_documents_answer(
    query_type: QueryType,
    documents: Vec<serde_json::Value>,
) -> QueryAnswerResponse {
    QueryAnswerResponse { query_type, answer_type: AnswerType::ConceptDocuments, answers: Some(documents) }
}
