/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, iter};

use axum_extra::headers::{Error as HeadersError, Header};
use http::{HeaderName, HeaderValue};

use crate::service::{
    http::message::header::{
        constants::{READ, SCHEMA, TRANSACTION_TYPE_HEADER, WRITE},
        decode_single,
    },
    TransactionType,
};

impl Header for TransactionType {
    fn name() -> &'static HeaderName {
        &TRANSACTION_TYPE_HEADER
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, HeadersError>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        match decode_single(values)?.to_lowercase().as_str() {
            SCHEMA => Ok(TransactionType::Schema),
            WRITE => Ok(TransactionType::Write),
            READ => Ok(TransactionType::Read),
            _ => Err(HeadersError::invalid()),
        }
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        values.extend(iter::once(self.into()));
    }
}

impl From<TransactionType> for HeaderValue {
    fn from(type_: TransactionType) -> Self {
        HeaderValue::from(&type_)
    }
}

impl From<&TransactionType> for HeaderValue {
    fn from(type_: &TransactionType) -> Self {
        HeaderValue::from_str(type_.to_string().as_str()).expect("Expected TransactionType -> HeaderValue conversion")
    }
}

impl fmt::Display for TransactionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionType::Schema => write!(f, "{}", SCHEMA),
            TransactionType::Write => write!(f, "{}", WRITE),
            TransactionType::Read => write!(f, "{}", READ),
        }
    }
}
