/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use error::typedb_error;

pub mod redundant_constraints;
pub mod relation_index;
pub mod transform;

typedb_error!(
    pub StaticOptimiserError(component = "Static optimiser", prefix = "SOP") {
        ConceptRead(1, "Error reading concept", typedb_source: Box<ConceptReadError>),
    }
);
