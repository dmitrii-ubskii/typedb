/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    extract::State,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, RequestPartsExt, Router,
};
use concurrency::TokioIntervalRunner;
use database::database_manager::DatabaseManager;
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use http::StatusCode;
use itertools::Itertools;
use options::TransactionOptions;
use resource::constants::{common::SECONDS_IN_MINUTE, server::DEFAULT_TRANSACTION_TIMEOUT_MILLIS};
use serde_json::json;
use system::concepts::{Credential, User};
use tokio::{
    sync::{
        mpsc::{channel, Sender},
        oneshot, RwLock,
    },
    time::timeout,
};
use user::{permission_manager::PermissionManager, user_manager::UserManager};
use uuid::Uuid;

use crate::{
    authentication::{credential_verifier::CredentialVerifier, token_manager::TokenManager, Accessor},
    service::{
        http::{
            diagnostics::{run_with_diagnostics, run_with_diagnostics_async},
            error::HTTPServiceError,
            message::{
                authentication::SigninPayload,
                body::JsonBody,
                database::{encode_database, encode_databases, DatabasePath},
                query::{QueryPayload, TransactionQueryPayload},
                transaction::{encode_transaction, TransactionId, TransactionOpenPayload, TransactionPath},
                user::{encode_user, encode_users, CreateUserPayload, UpdateUserPayload, UserPath},
                version::ProtocolVersion,
            },
            transaction_service,
            transaction_service::{QueryResponse, TransactionResponder, TransactionResponse, TransactionService},
        },
        transaction_service::{QueryType, TRANSACTION_REQUEST_BUFFER_SIZE},
    },
};

type TransactionRequestSender = Sender<(transaction_service::TransactionRequest, TransactionResponder)>;

#[derive(Debug, Clone)]
struct TransactionInfo {
    pub owner: String,
    pub request_sender: TransactionRequestSender,
}

#[derive(Debug, Clone)]
pub(crate) struct TypeDBService {
    address: SocketAddr,
    database_manager: Arc<DatabaseManager>,
    user_manager: Arc<UserManager>,
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
    transaction_services: Arc<RwLock<HashMap<Uuid, TransactionInfo>>>,
    shutdown_receiver: tokio::sync::watch::Receiver<()>,
    _transaction_cleanup_job: Arc<TokioIntervalRunner>,
}

impl TypeDBService {
    const TRANSACTION_CHECK_INTERVAL: Duration = Duration::from_secs(5 * SECONDS_IN_MINUTE);
    const QUERY_ENDPOINT_COMMIT_DEFAULT: bool = true;

    pub(crate) fn new(
        address: SocketAddr,
        database_manager: Arc<DatabaseManager>,
        user_manager: Arc<UserManager>,
        credential_verifier: Arc<CredentialVerifier>,
        token_manager: Arc<TokenManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        shutdown_receiver: tokio::sync::watch::Receiver<()>,
    ) -> Self {
        let transaction_request_senders = Arc::new(RwLock::new(HashMap::new()));

        let controlled_transactions = transaction_request_senders.clone();
        let transaction_cleanup_job = Arc::new(TokioIntervalRunner::new_with_initial_delay(
            move || {
                let transactions = controlled_transactions.clone();
                async move {
                    Self::cleanup_closed_transactions(transactions).await;
                }
            },
            Self::TRANSACTION_CHECK_INTERVAL,
            Self::TRANSACTION_CHECK_INTERVAL,
            false,
        ));

        Self {
            address,
            database_manager,
            user_manager,
            credential_verifier,
            token_manager,
            diagnostics_manager,
            transaction_services: transaction_request_senders,
            shutdown_receiver,
            _transaction_cleanup_job: transaction_cleanup_job,
        }
    }

    async fn cleanup_closed_transactions(transactions: Arc<RwLock<HashMap<Uuid, TransactionInfo>>>) {
        let mut transactions = transactions.write().await;
        transactions.retain(|_, info| !info.request_sender.is_closed());
    }

    pub(crate) fn address(&self) -> &SocketAddr {
        &self.address
    }

    async fn transaction_new(
        service: &TypeDBService,
        payload: TransactionOpenPayload,
    ) -> Result<(TransactionRequestSender, u64), HTTPServiceError> {
        let (request_sender, request_stream) = channel(TRANSACTION_REQUEST_BUFFER_SIZE);
        let options = payload.options.map(|options| options.into()).unwrap_or_else(|| TransactionOptions::default());
        let mut transaction_service = TransactionService::new(
            service.database_manager.clone(),
            service.diagnostics_manager.clone(),
            request_stream,
            service.shutdown_receiver.clone(),
        );

        let processing_time = transaction_service
            .open(payload.transaction_type, payload.database_name, options)
            .await
            .map_err(|typedb_source| HTTPServiceError::Transaction { typedb_source })?;

        tokio::spawn(async move { transaction_service.listen().await });
        Ok((request_sender, processing_time))
    }

    async fn transaction_request(
        sender: &TransactionRequestSender,
        request: transaction_service::TransactionRequest,
    ) -> Result<TransactionResponse, HTTPServiceError> {
        let (result_sender, result_receiver) = oneshot::channel();
        if let Err(_) = sender.send((request, TransactionResponder(result_sender))).await {
            return Err(HTTPServiceError::no_open_transaction());
        }

        match timeout(Duration::from_millis(DEFAULT_TRANSACTION_TIMEOUT_MILLIS), result_receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(HTTPServiceError::Internal { details: "channel closed".to_string() }),
            Err(_) => Err(HTTPServiceError::RequestTimeout {}),
        }
    }

    fn try_get_query_response(transaction_response: TransactionResponse) -> Result<QueryResponse, HTTPServiceError> {
        match transaction_response {
            TransactionResponse::Query(query_response) => Ok(query_response),
            TransactionResponse::Err(typedb_source) => Err(HTTPServiceError::Transaction { typedb_source }),
            TransactionResponse::Ok => {
                Err(HTTPServiceError::Internal { details: "unexpected transaction response".to_string() })
            }
        }
    }
}

impl TypeDBService {
    pub(crate) fn create_protected_router<T>(service: Arc<TypeDBService>) -> Router<T> {
        Router::new()
            .route("/:version/databases", get(Self::databases))
            .route("/:version/databases/:database-name", get(Self::databases_get))
            .route("/:version/databases/:database-name", post(Self::databases_create))
            .route("/:version/databases/:database-name", delete(Self::databases_delete))
            .route("/:version/databases/:database-name/schema", get(Self::databases_schema))
            .route("/:version/users", get(Self::users))
            .route("/:version/users/:username", get(Self::users_get))
            .route("/:version/users/:username", post(Self::users_create))
            .route("/:version/users/:username", put(Self::users_update))
            .route("/:version/users/:username", delete(Self::users_delete))
            .route("/:version/transactions/open", post(Self::transaction_open))
            .route("/:version/transactions/:transaction-id/commit", post(Self::transactions_commit))
            .route("/:version/transactions/:transaction-id/close", post(Self::transactions_close))
            .route("/:version/transactions/:transaction-id/rollback", post(Self::transactions_rollback))
            .route("/:version/transactions/:transaction-id/query", post(Self::transactions_query))
            .route("/:version/query", post(Self::query))
            .with_state(service)
    }

    pub(crate) fn create_unprotected_router<T>(service: Arc<TypeDBService>) -> Router<T> {
        Router::new()
            .route("/", get(Self::health))
            .route("/:version", get(Self::health))
            .route("/health", get(Self::health))
            .route("/:version/health", get(Self::health))
            .route("/:version/signin", post(Self::signin))
            .with_state(service)
    }

    async fn health() -> impl IntoResponse {
        StatusCode::NO_CONTENT
    }

    async fn signin(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        JsonBody(payload): JsonBody<SigninPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(service.diagnostics_manager.clone(), None::<&str>, ActionKind::SignIn, || async {
            service
                .credential_verifier
                .verify_password(&payload.username, &payload.password)
                .map_err(|typedb_source| HTTPServiceError::Authentication { typedb_source })?;
            Ok((StatusCode::OK, service.token_manager.new_token(payload.username).await))
        })
        .await
    }

    async fn databases(_version: ProtocolVersion, State(service): State<Arc<TypeDBService>>) -> impl IntoResponse {
        run_with_diagnostics(&service.diagnostics_manager, None::<&str>, ActionKind::DatabasesAll, || {
            Ok(JsonBody(encode_databases(service.database_manager.database_names())))
        })
    }

    async fn databases_get(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabasesContains,
            || {
                let database_name = service
                    .database_manager
                    .database(&database_path.database_name)
                    .ok_or(HTTPServiceError::NotFound {})?
                    .name()
                    .to_string();
                Ok(JsonBody(encode_database(database_name)))
            },
        )
    }

    async fn databases_create(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabasesCreate,
            || {
                service
                    .database_manager
                    .create_database(&database_path.database_name)
                    .map_err(|typedb_source| HTTPServiceError::DatabaseCreate { typedb_source })
            },
        )
    }

    async fn databases_delete(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        run_with_diagnostics(
            &service.diagnostics_manager,
            Some(&database_path.database_name),
            ActionKind::DatabaseDelete,
            || {
                service
                    .database_manager
                    .delete_database(&database_path.database_name)
                    .map_err(|typedb_source| HTTPServiceError::DatabaseDelete { typedb_source })
            },
        )
    }

    async fn databases_schema(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        database_path: DatabasePath,
    ) -> impl IntoResponse {
        StatusCode::NOT_IMPLEMENTED
    }

    // TODO: remove diagnsotics_async where not async!
    async fn users(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(service.diagnostics_manager.clone(), None::<&str>, ActionKind::UsersAll, || async {
            if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
                return Err(HTTPServiceError::operation_not_permitted());
            }
            Ok(JsonBody(encode_users(service.user_manager.all())))
        })
        .await
    }

    async fn users_get(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        user_path: UserPath,
    ) -> impl IntoResponse {
        run_with_diagnostics(&service.diagnostics_manager, None::<&str>, ActionKind::UsersContains, || {
            service
                .user_manager
                .get(&user_path.username)
                .map_err(|typedb_source| HTTPServiceError::UserGet { typedb_source })?
                .map(|(user, _)| JsonBody(encode_user(&user)))
                .ok_or(HTTPServiceError::NotFound {})
        })
    }

    async fn users_create(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        user_path: UserPath,
        JsonBody(payload): JsonBody<CreateUserPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            None::<&str>,
            ActionKind::UsersCreate,
            || async {
                if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
                    return Err(HTTPServiceError::operation_not_permitted());
                }
                let user = User { name: user_path.username };
                let credential = Credential::new_password(payload.password.as_str());
                service
                    .user_manager
                    .create(&user, &credential)
                    .map_err(|typedb_source| HTTPServiceError::UserCreate { typedb_source })
            },
        )
        .await
    }

    async fn users_update(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        user_path: UserPath,
        JsonBody(payload): JsonBody<UpdateUserPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            None::<&str>,
            ActionKind::UsersUpdate,
            || async {
                let user_update = None; // updating username is not supported now
                let credential_update = Some(Credential::new_password(&payload.password));
                let username = user_path.username.as_str();
                if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
                    return Err(HTTPServiceError::operation_not_permitted());
                }
                service
                    .user_manager
                    .update(username, &user_update, &credential_update)
                    .map_err(|typedb_source| HTTPServiceError::UserUpdate { typedb_source })?;
                service.token_manager.invalidate_user(username).await;
                Ok(())
            },
        )
        .await
    }

    async fn users_delete(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        user_path: UserPath,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            None::<&str>,
            ActionKind::UsersDelete,
            || async {
                let username = user_path.username.as_str();
                if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
                    return Err(HTTPServiceError::operation_not_permitted());
                }
                service
                    .user_manager
                    .delete(&user_path.username)
                    .map_err(|typedb_source| HTTPServiceError::UserDelete { typedb_source })?;
                service.token_manager.invalidate_user(username).await;
                Ok(())
            },
        )
        .await
    }

    async fn transaction_open(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        JsonBody(payload): JsonBody<TransactionOpenPayload>,
    ) -> impl IntoResponse {
        run_with_diagnostics_async(
            service.diagnostics_manager.clone(),
            Some(payload.database_name.clone()),
            ActionKind::TransactionOpen,
            || async {
                let (request_sender, _processing_time) = Self::transaction_new(&service, payload).await?;
                let uuid = Uuid::new_v4();
                service
                    .transaction_services
                    .write()
                    .await
                    .insert(uuid, TransactionInfo { owner: accessor, request_sender });
                Ok(JsonBody(encode_transaction(uuid)))
            },
        )
        .await
    }

    // TODO: Add diangostics to all these methods! Probably move out of transaction service if they exist there for "Query" coverage!

    async fn transactions_commit(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
    ) -> impl IntoResponse {
        let TransactionId(uuid) = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let transaction = senders.get(&uuid).ok_or(HTTPServiceError::no_open_transaction())?;
        if accessor != transaction.owner {
            return Err(HTTPServiceError::operation_not_permitted());
        }
        Self::transaction_request(&transaction.request_sender, transaction_service::TransactionRequest::Commit).await
    }

    async fn transactions_close(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
    ) -> impl IntoResponse {
        let TransactionId(uuid) = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let transaction = senders.get(&uuid).ok_or(HTTPServiceError::no_open_transaction())?;
        if accessor != transaction.owner {
            return Err(HTTPServiceError::operation_not_permitted());
        }
        Self::transaction_request(&transaction.request_sender, transaction_service::TransactionRequest::Close).await
    }

    async fn transactions_rollback(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
    ) -> impl IntoResponse {
        let TransactionId(uuid) = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let transaction = senders.get(&uuid).ok_or(HTTPServiceError::no_open_transaction())?;
        if accessor != transaction.owner {
            return Err(HTTPServiceError::operation_not_permitted());
        }
        Self::transaction_request(&transaction.request_sender, transaction_service::TransactionRequest::Rollback).await
    }

    async fn transactions_query(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        Accessor(accessor): Accessor,
        path: TransactionPath,
        JsonBody(payload): JsonBody<TransactionQueryPayload>,
    ) -> impl IntoResponse {
        let TransactionId(uuid) = path.transaction_id;
        let senders = service.transaction_services.read().await;
        let transaction = senders.get(&uuid).ok_or(HTTPServiceError::no_open_transaction())?;
        if accessor != transaction.owner {
            return Err(HTTPServiceError::operation_not_permitted());
        }
        Self::transaction_request(
            &transaction.request_sender,
            transaction_service::TransactionRequest::Query(payload.query),
        )
        .await
    }

    async fn query(
        _version: ProtocolVersion,
        State(service): State<Arc<TypeDBService>>,
        JsonBody(payload): JsonBody<QueryPayload>,
    ) -> impl IntoResponse {
        let (request_sender, _processing_time) =
            Self::transaction_new(&service, payload.transaction_open_payload).await?;

        let transaction_response =
            Self::transaction_request(&request_sender, transaction_service::TransactionRequest::Query(payload.query))
                .await?;
        let query_response = Self::try_get_query_response(transaction_response)?;

        // TODO: Move the default somewhere, probably rename
        let commit = match query_response.query_type() {
            QueryType::Read => false,
            QueryType::Write | QueryType::Schema => payload.commit.unwrap_or(Self::QUERY_ENDPOINT_COMMIT_DEFAULT),
        };

        let close_response = match commit {
            true => Self::transaction_request(&request_sender, transaction_service::TransactionRequest::Commit),
            false => Self::transaction_request(&request_sender, transaction_service::TransactionRequest::Close),
        }
        .await?;
        if let TransactionResponse::Err(typedb_source) = close_response {
            return match commit {
                true => Err(HTTPServiceError::QueryCommit { typedb_source }),
                false => Err(HTTPServiceError::QueryClose { typedb_source }),
            };
        }

        Ok(TransactionResponse::Query(query_response))
    }
}

trait IntoStatusCode {
    fn into_status_code(self) -> StatusCode;
}

impl IntoStatusCode for bool {
    fn into_status_code(self) -> StatusCode {
        match self {
            true => StatusCode::OK,
            false => StatusCode::NOT_FOUND,
        }
    }
}

// #[derive(Debug)]
// pub(crate) struct TypeDBService {
//     address: SocketAddr,
//     database_manager: Arc<DatabaseManager>,
//     user_manager: Arc<UserManager>,
//     token_manager: Arc<AuthenticatorCache>,
//     diagnostics_manager: Arc<DiagnosticsManager>,
//     shutdown_receiver: tokio::sync::watch::Receiver<()>,
// }
//
// impl TypeDBService {
//     pub(crate) fn new(
//         address: &SocketAddr,
//         database_manager: Arc<DatabaseManager>,
//         user_manager: Arc<UserManager>,
//         token_manager: Arc<AuthenticatorCache>,
//         diagnostics_manager: Arc<DiagnosticsManager>,
//         shutdown_receiver: tokio::sync::watch::Receiver<()>,
//     ) -> Self {
//         Self {
//             address: *address,
//             database_manager,
//             user_manager,
//             token_manager,
//             diagnostics_manager,
//             shutdown_receiver,
//         }
//     }
//
//     pub(crate) fn database_manager(&self) -> &DatabaseManager {
//         &self.database_manager
//     }
//
//     fn generate_connection_id(&self) -> ConnectionID {
//         Uuid::new_v4().into_bytes()
//     }
// }
//
// #[tonic::async_trait]
// impl typedb_protocol::type_db_server::TypeDb for TypeDBService {
//     async fn connection_open(
//         &self,
//         request: Request<typedb_protocol::connection::open::Req>,
//     ) -> Result<Response<typedb_protocol::connection::open::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::ConnectionOpen, || {
//             let receive_time = Instant::now();
//             let message = request.into_inner();
//             if message.version != typedb_protocol::Version::Version as i32 {
//                 let err = ProtocolError::IncompatibleProtocolVersion {
//                     server_protocol_version: typedb_protocol::Version::Version as i32,
//                     driver_protocol_version: message.version,
//                     driver_lang: message.driver_lang.clone(),
//                     driver_version: message.driver_version.clone(),
//                 };
//                 event!(Level::TRACE, "Rejected connection_open: {:?}", &err);
//                 return Err(err.into_status());
//             } else {
//                 event!(
//                     Level::TRACE,
//                     "Successful connection_open from '{}' version '{}'",
//                     &message.driver_lang,
//                     &message.driver_version
//                 );
//
//                 // generate a connection ID per 'connection_open' to be able to trace different connections by the same user
//                 Ok(Response::new(connection_open_res(
//                     self.generate_connection_id(),
//                     receive_time,
//                     database_all_res(&self.address, self.database_manager.database_names()),
//                 )))
//             }
//         })
//     }
//
//     async fn servers_all(&self, _request: Request<Req>) -> Result<Response<Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::ServersAll, || {
//             Ok(Response::new(servers_all_res(&self.address)))
//         })
//     }
//
//     async fn databases_all(
//         &self,
//         _request: Request<typedb_protocol::database_manager::all::Req>,
//     ) -> Result<Response<typedb_protocol::database_manager::all::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::DatabasesAll, || {
//             Ok(Response::new(database_all_res(&self.address, self.database_manager.database_names())))
//         })
//     }
//
//     async fn databases_get(
//         &self,
//         request: Request<typedb_protocol::database_manager::get::Req>,
//     ) -> Result<Response<typedb_protocol::database_manager::get::Res>, Status> {
//         let message = request.into_inner();
//         run_with_diagnostics(&self.diagnostics_manager, Some(message.name.clone()), ActionKind::DatabasesGet, || {
//             let database = self.database_manager.database(&message.name);
//             match database {
//                 None => {
//                     Err(ServiceError::DatabaseDoesNotExist { name: message.name }.into_error_message().into_status())
//                 }
//                 Some(_database) => Ok(Response::new(database_get_res(&self.address, message.name))),
//             }
//         })
//     }
//
//     async fn databases_contains(
//         &self,
//         request: Request<typedb_protocol::database_manager::contains::Req>,
//     ) -> Result<Response<typedb_protocol::database_manager::contains::Res>, Status> {
//         let message = request.into_inner();
//         run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabasesContains, || {
//             Ok(Response::new(database_contains_res(self.database_manager.database(&message.name).is_some())))
//         })
//     }
//
//     async fn databases_create(
//         &self,
//         request: Request<typedb_protocol::database_manager::create::Req>,
//     ) -> Result<Response<typedb_protocol::database_manager::create::Res>, Status> {
//         let message = request.into_inner();
//         run_with_diagnostics(&self.diagnostics_manager, Some(message.name.clone()), ActionKind::DatabasesCreate, || {
//             self.database_manager
//                 .create_database(message.name.clone())
//                 .map(|_| Response::new(database_create_res(message.name, &self.address)))
//                 .map_err(|err| err.into_error_message().into_status())
//         })
//     }
//
//     async fn database_schema(
//         &self,
//         request: Request<typedb_protocol::database::schema::Req>,
//     ) -> Result<Response<typedb_protocol::database::schema::Res>, Status> {
//         let message = request.into_inner();
//         run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabaseSchema, || {
//             Err(ServiceError::Unimplemented { description: "Database schema retrieval.".to_string() }
//                 .into_error_message()
//                 .into_status())
//         })
//     }
//
//     async fn database_type_schema(
//         &self,
//         request: Request<typedb_protocol::database::type_schema::Req>,
//     ) -> Result<Response<typedb_protocol::database::type_schema::Res>, Status> {
//         let message = request.into_inner();
//         run_with_diagnostics(&self.diagnostics_manager, Some(&message.name), ActionKind::DatabaseTypeSchema, || {
//             Err(ServiceError::Unimplemented { description: "Database schema (types only) retrieval.".to_string() }
//                 .into_error_message()
//                 .into_status())
//         })
//     }
//
//     async fn database_delete(
//         &self,
//         request: Request<typedb_protocol::database::delete::Req>,
//     ) -> Result<Response<typedb_protocol::database::delete::Res>, Status> {
//         let message = request.into_inner();
//         run_with_diagnostics(&self.diagnostics_manager, Some(message.name.clone()), ActionKind::DatabaseDelete, || {
//             self.database_manager
//                 .delete_database(message.name)
//                 .map(|_| Response::new(database_delete_res()))
//                 .map_err(|err| err.into_error_message().into_status())
//         })
//     }
//
//     async fn users_get(
//         &self,
//         request: Request<typedb_protocol::user_manager::get::Req>,
//     ) -> Result<Response<typedb_protocol::user_manager::get::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersGet, || {
//             let accessor = extract_username_field(request.metadata());
//             let get_req = request.into_inner();
//             if !PermissionManager::exec_user_get_permitted(accessor.as_str(), get_req.name.as_str()) {
//                 return Err(HTTPServiceError::operation_not_permitted().into_error_message().into_status());
//             }
//             match self.user_manager.get(get_req.name.as_str()) {
//                 Ok(get_result) => match get_result {
//                     Some((user, _)) => Ok(Response::new(users_get_res(user))),
//                     None => Err(ServiceError::UserDoesNotExist {}.into_error_message().into_status()),
//                 },
//                 Err(user_get_error) => Err(user_get_error.into_error_message().into_status()),
//             }
//         })
//     }
//
//     async fn users_all(
//         &self,
//         request: Request<typedb_protocol::user_manager::all::Req>,
//     ) -> Result<Response<typedb_protocol::user_manager::all::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersAll, || {
//             let accessor = extract_username_field(request.metadata());
//             if !PermissionManager::exec_user_all_permitted(accessor.as_str()) {
//                 return Err(HTTPServiceError::operation_not_permitted().into_error_message().into_status());
//             }
//             let users = self.user_manager.all();
//             Ok(Response::new(users_all_res(users)))
//         })
//     }
//
//     async fn users_contains(
//         &self,
//         request: Request<typedb_protocol::user_manager::contains::Req>,
//     ) -> Result<Response<typedb_protocol::user_manager::contains::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersContains, || {
//             let contains_req = request.into_inner();
//             self.user_manager
//                 .contains(contains_req.name.as_str())
//                 .map(|contains| Response::new(users_contains_res(contains)))
//                 .map_err(|err| err.into_error_message().into_status())
//         })
//     }
//
//     async fn users_create(
//         &self,
//         request: Request<typedb_protocol::user_manager::create::Req>,
//     ) -> Result<Response<typedb_protocol::user_manager::create::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersCreate, || {
//             let accessor = extract_username_field(request.metadata());
//             if !PermissionManager::exec_user_create_permitted(accessor.as_str()) {
//                 return Err(HTTPServiceError::operation_not_permitted().into_error_message().into_status());
//             }
//             users_create_req(request)
//                 .and_then(|(usr, cred)| self.user_manager.create(&usr, &cred))
//                 .map(|_| Response::new(user_create_res()))
//                 .map_err(|err| err.into_error_message().into_status())
//         })
//     }
//
//     async fn users_update(
//         &self,
//         request: Request<typedb_protocol::user::update::Req>,
//     ) -> Result<Response<typedb_protocol::user::update::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersUpdate, || {
//             let accessor = extract_username_field(request.metadata());
//             match users_update_req(request) {
//                 Ok((username, user_update, credential_update)) => {
//                     let username = username.as_str();
//                     if !PermissionManager::exec_user_update_permitted(accessor.as_str(), username) {
//                         return Err(HTTPServiceError::operation_not_permitted().into_error_message().into_status());
//                     }
//                     match self.user_manager.update(username, &user_update, &credential_update) {
//                         Ok(()) => {
//                             self.token_manager.invalidate_user(username);
//                             Ok(Response::new(user_update_res()))
//                         }
//                         Err(user_update_err) => Err(user_update_err.into_error_message().into_status()),
//                     }
//                 }
//                 Err(user_update_err) => Err(user_update_err.into_error_message().into_status()),
//             }
//         })
//     }
//
//     async fn users_delete(
//         &self,
//         request: Request<typedb_protocol::user::delete::Req>,
//     ) -> Result<Response<typedb_protocol::user::delete::Res>, Status> {
//         run_with_diagnostics(&self.diagnostics_manager, None::<&str>, ActionKind::UsersDelete, || {
//             let accessor = extract_username_field(request.metadata());
//             let delete_req = request.into_inner();
//             let username = delete_req.name.as_str();
//             if !PermissionManager::exec_user_delete_allowed(accessor.as_str(), username) {
//                 return Err(HTTPServiceError::operation_not_permitted().into_error_message().into_status());
//             }
//             let result = self.user_manager.delete(username);
//             match result {
//                 Ok(_) => {
//                     self.token_manager.invalidate_user(username);
//                     Ok(Response::new(users_delete_res()))
//                 }
//                 Err(e) => Err(e.into_error_message().into_status()),
//             }
//         })
//     }
//
//     type transactionStream = Pin<Box<ReceiverStream<Result<Server, Status>>>>;
//
//     async fn transaction(
//         &self,
//         request: Request<Streaming<Client>>,
//     ) -> Result<Response<Self::transactionStream>, Status> {
//         let request_stream = request.into_inner();
//         let (response_sender, response_receiver) = channel(TRANSACTION_REQUEST_BUFFER_SIZE);
//         let mut service = TransactionService::new(
//             request_stream,
//             response_sender,
//             self.database_manager.clone(),
//             self.diagnostics_manager.clone(),
//             self.shutdown_receiver.clone(),
//         );
//         tokio::spawn(async move { service.listen().await });
//         let stream: ReceiverStream<Result<Server, Status>> = ReceiverStream::new(response_receiver);
//         Ok(Response::new(Box::pin(stream)))
//     }
// }
//
// fn extract_username_field(metadata: &MetadataMap) -> String {
//     metadata
//         .get(AUTHENTICATOR_USERNAME_FIELD)
//         .map(|u| u.to_str())
//         .expect(format!("Unable to find expected field in the metadata: {}", AUTHENTICATOR_USERNAME_FIELD).as_str())
//         .expect(format!("Unable to parse value from the {} field", AUTHENTICATOR_USERNAME_FIELD).as_str())
//         .to_string()
// }
