/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use primitive::maybe_owns::MaybeOwns;

pub trait StructFieldsIndex {
    fn get_struct_fields(&self, name: &str) -> Result<Option<MaybeOwns<'_, ()>>, ()>;
}

impl StructFieldsIndex for () {
    fn get_struct_fields(&self, name: &str) -> Result<Option<MaybeOwns<'_, ()>>, ()> {
        todo!()
    }
}
