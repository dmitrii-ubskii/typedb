/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use serde::Deserialize;

use crate::service::http::message::transaction::TransactionOpenPayload;

pub(crate) mod concept;
pub(crate) mod document;
pub(crate) mod row;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TransactionQueryPayload {
    pub(crate) query: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueryPayload {
    pub(crate) query: String,
    pub(crate) commit: Option<bool>,

    #[serde(flatten)]
    pub(crate) transaction_open_payload: TransactionOpenPayload,
}
