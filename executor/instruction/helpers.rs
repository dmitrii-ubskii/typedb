/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(super) mod sort_mode {
    pub(super) const ON_CANONICAL_FROM: TupleSortMode = TupleSortMode::From;
    pub(super) const ON_CANONICAL_TO: TupleSortMode = TupleSortMode::To;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum TupleSortMode {
        From,
        To,
    }
}

// Introduce as you go
pub(super) trait IsSortedOnCanonicalFrom : Into<IteratorSortedOnCanonicalFrom> { }
pub(super) trait IsSortedOnCanonicalTo : Into<IteratorSortedOnCanonicalFrom> { }

pub(super) enum IteratorSortedOnCanonicalFrom {

}

pub(super) enum IteratorSortedOnCanonicalTo {

}






// Introduce as you go


