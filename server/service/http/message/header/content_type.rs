/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, iter};

use axum_extra::headers::{Error as HeadersError, Header};
use http::{HeaderName, HeaderValue};

use crate::service::http::message::header::{
    constants::{APPLICATION_JSON, TEXT_PLAIN},
    decode_single,
};

#[derive(Debug, Clone, Copy, Hash, Ord, PartialOrd, PartialEq, Eq)]
pub enum ContentType {
    TextPlain,
    ApplicationJson,
}

impl Header for ContentType {
    fn name() -> &'static HeaderName {
        &http::header::CONTENT_TYPE
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, HeadersError>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        match decode_single(values)? {
            TEXT_PLAIN => Ok(ContentType::TextPlain),
            APPLICATION_JSON => Ok(ContentType::ApplicationJson),
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

impl From<ContentType> for HeaderValue {
    fn from(type_: ContentType) -> Self {
        HeaderValue::from(&type_)
    }
}

impl From<&ContentType> for HeaderValue {
    fn from(type_: &ContentType) -> Self {
        HeaderValue::from_str(type_.to_string().as_str()).expect("Expected ContentType -> HeaderValue conversion")
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentType::TextPlain => write!(f, "{}", TEXT_PLAIN),
            ContentType::ApplicationJson => write!(f, "{}", APPLICATION_JSON),
        }
    }
}
