/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::type_::edge::TypeEdgeEncoding;
use encoding::layout::prefix::Prefix;
use crate::type_::TypeAPI;


#[derive(Clone)]
pub struct Sub<T> {
    subtype: T,
    supertype: T,
}

impl<'a, T: TypeAPI<'a>> Sub<T> {

    pub(crate) fn subtype(&self) -> T {
        self.subtype.clone()
    }

    pub(crate) fn supertype(&self) -> T {
        self.supertype.clone()
    }
}

impl<'a, T: TypeAPI<'a>> TypeEdgeEncoding<'a> for Sub<T> {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgeSub;
    const REVERSE_PREFIX: Prefix = Prefix::EdgeSubReverse;
    type From = T;
    type To = T;

    fn from_vertices(from: T, to: T) -> Self {
        Sub { subtype: from, supertype: to,  }
    }

    fn canonical_from(&self) -> Self::From {
        self.subtype()
    }

    fn canonical_to(&self) -> Self::To {
        self.supertype()
    }
}
