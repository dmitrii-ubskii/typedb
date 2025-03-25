/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::collections::HashSet;

use cucumber::{gherkin::Step, given, then, when};
use futures::{
    future::{join_all, try_join_all},
    stream, StreamExt, TryFutureExt,
};
use hyper::{client::HttpConnector, Client};
use itertools::Itertools;
use macro_rules_attribute::apply;
use params;
use tokio::time::sleep;

use crate::{
    generic_step, in_background,
    message::{databases, databases_create, databases_delete},
    util::iter_table,
    Context, HttpContext,
};

async fn create_database(context: &HttpContext, name: String, may_error: params::MayError) {
    may_error.check(databases_create(context, &name).await);
}

async fn delete_database(context: &HttpContext, name: &str, may_error: params::MayError) {
    may_error.check(databases_delete(context, &name).await);
}

async fn has_database(context: &HttpContext, name: &str) -> bool {
    databases(context).await.unwrap().databases.iter().find(|database| database.name == name).is_some()
}

#[apply(generic_step)]
#[step(expr = "connection create database: {word}{may_error}")]
pub async fn connection_create_database(context: &mut Context, name: String, may_error: params::MayError) {
    create_database(&context.http_context, name, may_error).await;
}

#[apply(generic_step)]
#[step(expr = "connection create database with empty name{may_error}")]
pub async fn connection_create_database_with_an_empty_name(context: &mut Context, may_error: params::MayError) {
    create_database(&context.http_context, "".to_string(), may_error).await;
}

#[apply(generic_step)]
#[step(expr = "connection create database(s):")]
async fn connection_create_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step) {
        create_database(&context.http_context, name.into(), params::MayError::False).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection create databases in parallel:")]
async fn connection_create_databases_in_parallel(context: &mut Context, step: &Step) {
    join_all(iter_table(step).map(|name| databases_create(&context.http_context, name))).await;
}

#[apply(generic_step)]
#[step(expr = "in background, connection create database: {word}{may_error}")]
pub async fn in_background_connection_create_database(
    context: &mut Context,
    name: String,
    may_error: params::MayError,
) {
    in_background!(context, |background| {
        create_database(&background, name, may_error).await;
    });
}

#[apply(generic_step)]
#[step(expr = "connection delete database: {word}{may_error}")]
pub async fn connection_delete_database(context: &mut Context, name: String, may_error: params::MayError) {
    delete_database(&context.http_context, &name, may_error).await;
}

#[apply(generic_step)]
#[step(expr = "connection delete database(s):")]
async fn connection_delete_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step) {
        delete_database(&context.http_context, name, params::MayError::False).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection delete databases in parallel:")]
async fn connection_delete_databases_in_parallel(context: &mut Context, step: &Step) {
    try_join_all(iter_table(step).map(|name| databases_delete(&context.http_context, &name))).await.unwrap();
}

#[apply(generic_step)]
#[step(expr = "in background, connection delete database: {word}{may_error}")]
pub async fn in_background_connection_delete_database(
    context: &mut Context,
    name: String,
    may_error: params::MayError,
) {
    in_background!(context, |background| {
        delete_database(&background, &name, may_error).await;
    });
}

#[apply(generic_step)]
#[step(expr = "connection has database: {word}")]
async fn connection_has_database(context: &mut Context, name: String) {
    assert!(has_database(&context.http_context, &name).await, "Connection doesn't contain database {name}.",);
}

#[apply(generic_step)]
#[step(expr = "connection has database(s):")]
async fn connection_has_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step).map(|name| name.to_owned()) {
        assert!(
            has_database(&context.http_context, &name).await,
            "Connection doesn't contain at least one of the databases.",
        );
    }
}

#[apply(generic_step)]
#[step(expr = "connection does not have database: {word}")]
async fn connection_does_not_have_database(context: &mut Context, name: String) {
    assert!(!has_database(&context.http_context, &name).await, "Connection contains database {name}.",);
}

#[apply(generic_step)]
#[step(expr = "connection does not have database(s):")]
async fn connection_does_not_have_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step).map(|name| name.to_owned()) {
        assert!(
            !has_database(&context.http_context, &name).await,
            "Connection doesn't contain at least one of the databases.",
        );
    }
}
