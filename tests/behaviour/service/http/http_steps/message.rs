/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use hyper::{
    client::HttpConnector,
    header::{AUTHORIZATION, CONTENT_TYPE},
    Body, Client, Method, Request, Uri,
};
use serde_json::json;
use server::service::http::message::{
    database::{DatabaseResponse, DatabasesResponse},
    transaction::TransactionResponse,
    user::{UserResponse, UsersResponse},
};
use url::form_urlencoded;

use crate::{Context, HttpBehaviourTestError, HttpContext};

async fn send_request(
    context: &HttpContext,
    method: Method,
    url: &str,
    body: Option<&str>,
) -> Result<String, HttpBehaviourTestError> {
    let uri: Uri = url.parse().expect("Invalid URI");
    let mut req = Request::builder().method(method).uri(uri);

    if let Some(token) = context.auth_token.as_ref() {
        req = req.header(AUTHORIZATION, format!("Bearer {}", token));
    }

    let req = req
        .header(CONTENT_TYPE, "application/json")
        .body(match body {
            Some(b) => Body::from(b.to_string()),
            None => Body::empty(),
        })
        .map_err(|source| HttpBehaviourTestError::HttpError(source))?;

    let res = context.http_client.request(req).await.map_err(HttpBehaviourTestError::HyperError)?;

    let status = res.status();
    let body_bytes = hyper::body::to_bytes(res.into_body()).await.map_err(HttpBehaviourTestError::HyperError)?;

    let body_str = String::from_utf8_lossy(&body_bytes).to_string();

    if !status.is_success() {
        return Err(HttpBehaviourTestError::StatusError { code: status, message: body_str });
    }

    Ok(body_str)
}

pub async fn check_health(http_client: Client<HttpConnector>) -> Result<String, HttpBehaviourTestError> {
    let uri: Uri = format!("{}/health", Context::default_versioned_endpoint()).parse().expect("Invalid URI");
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .map_err(|source| HttpBehaviourTestError::HttpError(source))?;
    let res = http_client.request(req).await.map_err(|source| HttpBehaviourTestError::HyperError(source))?;
    let body_bytes =
        hyper::body::to_bytes(res.into_body()).await.map_err(|source| HttpBehaviourTestError::HyperError(source))?;
    Ok(String::from_utf8_lossy(&body_bytes).to_string())
}

pub async fn authenticate_default(context: &HttpContext) -> String {
    authenticate(
        context,
        Context::default_versioned_endpoint().as_str(),
        Context::ADMIN_USERNAME,
        Context::ADMIN_PASSWORD,
    )
    .await
    .expect("Expected default auth")
}

pub async fn authenticate(
    context: &HttpContext,
    endpoint: &str,
    username: &str,
    password: &str,
) -> Result<String, HttpBehaviourTestError> {
    let url = format!("{}/signin", endpoint);
    let json_body = json!({
        "username": username,
        "password": password,
    });
    send_request(context, Method::POST, &url, Some(json_body.to_string().as_str())).await
}

pub async fn databases(context: &HttpContext) -> Result<DatabasesResponse, HttpBehaviourTestError> {
    let url = format!("{}/databases", Context::default_versioned_endpoint());
    let response = send_request(context, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn databases_get(
    context: &HttpContext,
    database_name: &str,
) -> Result<DatabaseResponse, HttpBehaviourTestError> {
    let url = format!("{}/databases/{}", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(context, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn databases_create(context: &HttpContext, database_name: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/databases/{}", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(context, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn databases_delete(context: &HttpContext, database_name: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/databases/{}", Context::default_versioned_endpoint(), encode_path_variable(database_name));
    let response = send_request(context, Method::DELETE, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn users(context: &HttpContext) -> Result<UsersResponse, HttpBehaviourTestError> {
    let url = format!("{}/users", Context::default_versioned_endpoint());
    let response = send_request(context, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn users_get(context: &HttpContext, username: &str) -> Result<UserResponse, HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let response = send_request(context, Method::GET, &url, None).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn users_create(context: &HttpContext, username: &str, password: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let json_body = json!({
        "password": password,
    });
    let response = send_request(context, Method::POST, &url, Some(json_body.to_string().as_str())).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn users_update(context: &HttpContext, username: &str, password: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let json_body = json!({
        "password": password,
    });
    let response = send_request(context, Method::PUT, &url, Some(json_body.to_string().as_str())).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn users_delete(context: &HttpContext, username: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/users/{}", Context::default_versioned_endpoint(), encode_path_variable(username));
    let response = send_request(context, Method::DELETE, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_open(
    context: &HttpContext,
    database_name: &str,
    transaction_type: &str,
) -> Result<TransactionResponse, HttpBehaviourTestError> {
    let url = format!("{}/transactions/open", Context::default_versioned_endpoint());
    let json_body = json!({
        "databaseName": database_name,
        "transactionType": transaction_type,
    });
    let response = send_request(context, Method::POST, &url, Some(json_body.to_string().as_str())).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn transactions_close(context: &HttpContext, transaction_id: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/close", Context::default_versioned_endpoint(), transaction_id);
    let response = send_request(context, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_commit(context: &HttpContext, transaction_id: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/commit", Context::default_versioned_endpoint(), transaction_id);
    let response = send_request(context, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_rollback(context: &HttpContext, transaction_id: &str) -> Result<(), HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/rollback", Context::default_versioned_endpoint(), transaction_id);
    let response = send_request(context, Method::POST, &url, None).await?;
    assert!(response.is_empty(), "Expected empty response, got {response} instead");
    Ok(())
}

pub async fn transactions_query(
    context: &HttpContext,
    transaction_id: &str,
    query: &str,
) -> Result<serde_json::Value, HttpBehaviourTestError> {
    let url = format!("{}/transactions/{}/query", Context::default_versioned_endpoint(), transaction_id);
    let json_body = json!({
        "query": query,
    });
    let response = send_request(context, Method::POST, &url, Some(json_body.to_string().as_str())).await?;
    Ok(serde_json::from_str(&response).expect("Expected a json body"))
}

pub async fn query(context: &HttpContext, transaction_type: &str, query: &str) -> Result<(), HttpBehaviourTestError> {
    // let url = format!("{}/query", Context::default_versioned_endpoint());
    // send_request(context, Method::POST, &url, Some(typeql)).await?;
    Ok(())
}

fn encode_path_variable(var: &str) -> String {
    form_urlencoded::byte_serialize(var.as_bytes()).collect()
}
