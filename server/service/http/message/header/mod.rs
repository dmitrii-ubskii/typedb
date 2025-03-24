/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use axum_extra::headers::{Error as HeadersError, Header};
use http::HeaderValue;

pub mod accept;
pub mod constants;
pub mod content_type;
pub mod database;
pub mod transaction_type;

pub fn decode_single<'i, I>(values: &mut I) -> Result<&'i str, HeadersError>
where
    I: Iterator<Item = &'i HeaderValue>,
{
    let first_value = values.next().ok_or_else(HeadersError::invalid)?.to_str().map_err(|_| HeadersError::invalid())?;
    Ok(first_value)
}
