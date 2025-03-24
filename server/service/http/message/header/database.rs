/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, iter};

use axum_extra::headers::{Error as HeadersError, Header};
use http::{HeaderName, HeaderValue};

use crate::service::http::message::header::{
    constants::{APPLICATION_JSON, DATABASE_HEADER, TEXT_PLAIN},
    decode_single,
};

pub struct Database(String);

impl Header for Database {
    fn name() -> &'static HeaderName {
        &DATABASE_HEADER
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, HeadersError>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = decode_single(values)?;
        Ok(Database(value.to_string()))
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        values.extend(iter::once(self.into()));
    }
}

impl From<Database> for HeaderValue {
    fn from(database: Database) -> Self {
        HeaderValue::from(&database)
    }
}

impl From<&Database> for HeaderValue {
    fn from(database: &Database) -> Self {
        let Database(database_str) = database;
        HeaderValue::from_str(database_str.as_str()).expect("Expected DatabaseHeader -> HeaderValue conversion")
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
