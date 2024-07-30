/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use crate::define::DefineError;

#[derive(Debug)]
pub enum QueryError {
    ParseError { typeql_query: String, source: typeql::common::Error },
    Define { source: DefineError },
}

impl fmt::Display for QueryError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for QueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            QueryError::ParseError { source, .. } => Some(source),
            QueryError::Define { source, .. } => Some(source),
        }
    }
}
