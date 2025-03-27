/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use axum::{
    async_trait,
    extract::FromRequestParts,
    response::{IntoResponse, Response},
};
use http::request::Parts;
use serde::Deserialize;

use crate::{authentication::Accessor, service::http::error::HTTPServiceError};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SigninPayload {
    pub username: String,
    pub password: String,
}

#[async_trait]
impl<S> FromRequestParts<S> for Accessor
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Accessor::from_extensions(&parts.extensions)
            .map_err(|typedb_source| HTTPServiceError::Authentication { typedb_source }.into_response())
    }
}
