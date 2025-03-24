/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::{
    ControlFlow,
    ControlFlow::{Break, Continue},
};

pub(crate) trait ControlFlowExt<B, C> {
    fn map<T, F: FnOnce(B) -> T>(self, f: F) -> ControlFlow<T, C>;
}

impl<B, C> ControlFlowExt<B, C> for ControlFlow<B, C> {
    fn map<T, F: FnOnce(B) -> T>(self, f: F) -> ControlFlow<T, C> {
        match self {
            Continue(c) => Continue(c),
            Break(b) => Break(f(b)),
        }
    }
}

trait ControlFlowResultExt<T, E> {
    fn transpose(self) -> ControlFlow<Result<T, E>>;
}

impl<T, E> ControlFlowResultExt<T, E> for Result<ControlFlow<T>, E> {
    fn transpose(self) -> ControlFlow<Result<T, E>> {
        match self {
            Ok(Continue(())) => Continue(()),
            Ok(Break(t)) => Break(Ok(t)),
            Err(err) => Break(Err(err)),
        }
    }
}

trait ResultControlFlowExt<T, E> {
    fn transpose(self) -> Result<ControlFlow<T>, E>;
}

impl<T, E> ResultControlFlowExt<T, E> for ControlFlow<Result<T, E>> {
    fn transpose(self) -> Result<ControlFlow<T>, E> {
        match self {
            Continue(()) => Ok(Continue(())),
            Break(Ok(t)) => Ok(Break(t)),
            Break(Err(err)) => Err(err),
        }
    }
}
