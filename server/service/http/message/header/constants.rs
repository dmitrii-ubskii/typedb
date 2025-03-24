/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use http::HeaderName;

pub const TEXT_PLAIN: &str = "text/plain";
pub const APPLICATION_JSON: &str = "application/json";
pub const DATABASE: &str = "database";
pub const TRANSACTION_TYPE: &str = "transaction_type";
pub const TRANSACTION_ID: &str = "transaction_id";

pub const SCHEMA: &str = "schema";
pub const WRITE: &str = "write";
pub const READ: &str = "read";

pub static DATABASE_HEADER: HeaderName = HeaderName::from_static(DATABASE);
pub static TRANSACTION_TYPE_HEADER: HeaderName = HeaderName::from_static(TRANSACTION_TYPE);
