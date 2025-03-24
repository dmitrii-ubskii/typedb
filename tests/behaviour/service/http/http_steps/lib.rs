/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    fmt,
    fmt::Formatter,
    iter, mem,
    path::{Path, PathBuf},
    str::FromStr,
};

use cucumber::{gherkin::Feature, StatsWriter, World};
use futures::{
    future::{try_join_all, Either},
    stream::{self, StreamExt},
};
use hyper::{client::HttpConnector, Client};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use itertools::Itertools;
use options::TransactionOptions;
use tokio::time::{sleep, Duration};

use crate::params::QueryAnswerType;

mod connection;
mod params;
mod query;
mod util;

#[derive(Debug, Default)]
struct SingletonParser {
    basic: cucumber::parser::Basic,
}

impl<I: AsRef<Path>> cucumber::Parser<I> for SingletonParser {
    type Cli = <cucumber::parser::Basic as cucumber::Parser<I>>::Cli;
    type Output = stream::FlatMap<
        stream::Iter<std::vec::IntoIter<Result<Feature, cucumber::parser::Error>>>,
        Either<
            stream::Iter<std::vec::IntoIter<Result<Feature, cucumber::parser::Error>>>,
            stream::Iter<iter::Once<Result<Feature, cucumber::parser::Error>>>,
        >,
        fn(
            Result<Feature, cucumber::parser::Error>,
        ) -> Either<
            stream::Iter<std::vec::IntoIter<Result<Feature, cucumber::parser::Error>>>,
            stream::Iter<iter::Once<Result<Feature, cucumber::parser::Error>>>,
        >,
    >;

    fn parse(self, input: I, cli: Self::Cli) -> Self::Output {
        self.basic.parse(input, cli).flat_map(|res| match res {
            Ok(mut feature) => {
                let scenarios = mem::take(&mut feature.scenarios);
                let singleton_features = scenarios
                    .into_iter()
                    .map(|scenario| {
                        Ok(Feature {
                            name: feature.name.clone() + " :: " + &scenario.name,
                            scenarios: vec![scenario],
                            ..feature.clone()
                        })
                    })
                    .collect_vec();
                Either::Left(stream::iter(singleton_features))
            }
            Err(err) => Either::Right(stream::iter(iter::once(Err(err)))),
        })
    }
}

#[derive(World)]
pub struct Context {
    pub tls_root_ca: PathBuf,
    pub transaction_options: TransactionOptions,
    pub http_client: Client<HttpConnector>,
    pub transaction_ids: VecDeque<String>,
    pub answer: Option<String>,
    pub answer_type: Option<String>,
    pub answer_query_type: Option<String>,
    // pub collected_rows: Option<Vec<ConceptRow>>,
    pub collected_documents: Option<Vec<serde_json::Value>>,
    pub concurrent_answers: Vec<String>,
    // pub concurrent_rows_streams: Option<Vec<BoxStream<'static, Result<(), HttpBehaviourTestError><ConceptRow>>>>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("tls_root_ca", &self.tls_root_ca)
            .field("transaction_options", &self.transaction_options)
            .field("transactions", &self.transaction_ids)
            .field("answer", &self.answer)
            .field("answer_type", &self.answer_type)
            .field("answer_query_type", &self.answer_query_type)
            // .field("collected_rows", &self.collected_rows)
            .field("collected_documents", &self.collected_documents)
            .field("concurrent_answers", &self.concurrent_answers)
            // .field(
            //     "concurrent_rows_streams",
            //     &self.concurrent_rows_streams.as_ref().map(|streams| format!("{} streams", streams.len())),
            // )
            .finish()
    }
}

impl Context {
    const DEFAULT_ADDRESS: &'static str = "127.0.0.1:8000";
    const DEFAULT_DATABASE: &'static str = "test";
    const ADMIN_USERNAME: &'static str = "admin";
    const ADMIN_PASSWORD: &'static str = "password";
    const STEP_REATTEMPT_SLEEP: Duration = Duration::from_millis(250);
    const STEP_REATTEMPT_LIMIT: u32 = 20;

    pub async fn test<I: AsRef<Path>>(glob: I) -> bool {
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));

        !Self::cucumber::<I>()
            .repeat_failed()
            .fail_on_skipped()
            .max_concurrent_scenarios(Some(1))
            .with_parser(SingletonParser::default())
            .with_default_cli()
            .before(move |_, _, _, _| {
                // cucumber removes the default hook before each scenario and restores it after!
                std::panic::set_hook(Box::new(move |info| println!("{}", info)));
                Box::pin(async move {})
            })
            .after(|_, _, _, _, context| {
                Box::pin(async {
                    context.unwrap().after_scenario().await;
                })
            })
            .filter_run(glob, |_, _, sc| {
                sc.name.contains(std::env::var("SCENARIO_FILTER").as_deref().unwrap_or(""))
                    && !sc.tags.iter().any(Self::is_ignore_tag)
            })
            .await
            .execution_has_failed()
    }

    fn is_ignore_tag(t: &String) -> bool {
        t == "ignore" || t == "ignore-typedb-driver" || t == "ignore-typedb-driver-rust"
    }

    pub async fn after_scenario(&mut self) {
        sleep(Context::STEP_REATTEMPT_SLEEP).await;
        self.cleanup_transactions().await;
        self.cleanup_databases().await;
        self.cleanup_users().await;
        self.cleanup_answers().await;
        self.cleanup_concurrent_answers().await;
    }

    pub async fn all_databases(&self) -> HashSet<String> {
        // TODO: Get databases
        // self.driver
        //     .as_ref()
        //     .unwrap()
        //     .databases()
        //     .all()
        //     .await
        //     .unwrap()
        //     .into_iter()
        //     .map(|db| db.name().to_owned())
        //     .collect::<HashSet<_>>()
        HashSet::new()
    }

    pub async fn cleanup_databases(&mut self) {
        // TODO: Get databases, for each delete
        // try_join_all(self.driver.as_ref().unwrap().databases().all().await.unwrap().into_iter().map(|db| db.delete()))
        //     .await
        //     .unwrap();
    }

    pub async fn cleanup_transactions(&mut self) {
        while let Some(transaction) = self.try_take_transaction() {
            // TODO: Close transactions
            // transaction.force_close();
        }
    }

    pub async fn cleanup_users(&mut self) {
        // TODO: Implement
        // try_join_all(
        //     self.driver
        //         .as_ref()
        //         .unwrap()
        //         .users()
        //         .all()
        //         .await
        //         .unwrap()
        //         .into_iter()
        //         .filter(|user| user.name != Context::ADMIN_USERNAME)
        //         .map(|user| user.delete()),
        // )
        // .await
        // .expect("Expected users cleanup");

        // TODO: Return
        // self.driver.as_ref().unwrap().users().set_password(Context::ADMIN_USERNAME, Context::ADMIN_PASSWORD).await.unwrap();
    }

    pub async fn cleanup_answers(&mut self) {
        self.answer = None;
        self.answer_type = None;
        self.answer_query_type = None;
        // self.collected_rows = None;
        self.collected_documents = None;
    }

    pub async fn cleanup_concurrent_answers(&mut self) {
        self.concurrent_answers = Vec::new();
        // self.concurrent_rows_streams = None;
    }

    pub fn transaction_opt(&self) -> Option<&String> {
        self.transaction_ids.get(0)
    }

    pub fn transaction(&self) -> &String {
        self.transaction_ids.get(0).unwrap()
    }

    pub fn take_transaction(&mut self) -> String {
        self.transaction_ids.pop_front().unwrap()
    }

    pub fn try_take_transaction(&mut self) -> Option<String> {
        self.transaction_ids.pop_front()
    }

    pub fn push_transaction(
        &mut self,
        transaction: Result<String, HttpBehaviourTestError>,
    ) -> Result<(), HttpBehaviourTestError> {
        self.transaction_ids.push_back(transaction?);
        Ok(())
    }

    pub async fn set_transactions(&mut self, transactions: VecDeque<String>) {
        self.cleanup_transactions().await;
        self.transaction_ids = transactions;
    }

    pub fn set_answer(&mut self, answer: Result<String, HttpBehaviourTestError>) -> Result<(), HttpBehaviourTestError> {
        let answer = answer?;
        // self.answer_query_type = Some(answer.get_query_type());
        // self.answer_type = Some(match &answer {
        //     QueryAnswer::Ok(_) => QueryAnswerType::Ok,
        //     QueryAnswer::ConceptRowStream(_, _) => QueryAnswerType::ConceptRows,
        //     QueryAnswer::ConceptDocumentStream(_, _) => QueryAnswerType::ConceptDocuments,
        // });
        self.answer = Some(answer);
        // TODO: Do something
        Ok(())
    }

    // pub fn set_concurrent_answers(&mut self, answers: Vec<QueryAnswer>) {
    //     self.concurrent_answers = answers;
    // }

    // pub async fn unwrap_answer_if_needed(&mut self) {
    //     if self.collected_rows.is_none() && self.collected_documents.is_none() {
    //         match self.answer_type.expect("Nothing to unwrap: no answer") {
    //             QueryAnswerType::Ok => panic!("Nothing to unwrap: cannot unwrap Ok"),
    //             QueryAnswerType::ConceptRows => self.unwrap_answer_into_rows().await,
    //             QueryAnswerType::ConceptDocuments => self.unwrap_answer_into_documents().await,
    //         }
    //     }
    // }

    // pub async fn unwrap_concurrent_answers_into_rows_streams_if_neeed(&mut self) {
    //     if self.concurrent_rows_streams.is_none() {
    //         self.unwrap_concurrent_answers_into_rows_streams().await
    //     }
    // }

    // pub async fn unwrap_answer_into_rows(&mut self) {
    //     self.collected_rows =
    //         Some(self.answer.take().unwrap().into_rows().map(|result| result.unwrap()).collect::<Vec<_>>().await);
    // }

    // pub async fn unwrap_concurrent_answers_into_rows_streams(&mut self) {
    //     self.concurrent_rows_streams =
    //         Some(self.concurrent_answers.drain(..).map(|answer| answer.into_rows()).collect());
    // }

    // pub async fn unwrap_answer_into_documents(&mut self) {
    //     self.collected_documents =
    //         Some(self.answer.take().unwrap().into_documents().map(|result| result.unwrap()).collect::<Vec<_>>().await);
    // }

    pub async fn get_answer(&self) -> Option<&String> {
        self.answer.as_ref()
    }

    pub async fn get_answer_query_type(&self) -> Option<&String> {
        self.answer_query_type.as_ref()
    }

    pub async fn get_answer_type(&self) -> Option<QueryAnswerType> {
        self.answer_type
            .clone()
            .map(|str| QueryAnswerType::from_str(str.as_str()).expect("Expected conversion to query answer type"))
        // TODO: ???
    }

    // pub async fn try_get_collected_rows(&mut self) -> Option<&Vec<ConceptRow>> {
    //     self.unwrap_answer_if_needed().await;
    //     self.collected_rows.as_ref()
    // }

    // pub async fn get_collected_rows(&mut self) -> &Vec<ConceptRow> {
    //     self.try_get_collected_rows().await.unwrap()
    // }

    // pub async fn try_get_collected_documents(&mut self) -> Option<&Vec<serde_json::Value>> {
    //     self.unwrap_answer_if_needed().await;
    //     self.collected_documents.as_ref()
    // }
    //
    // pub async fn get_collected_documents(&mut self) -> &Vec<serde_json::Value> {
    //     self.try_get_collected_documents().await.unwrap()
    // }

    // pub async fn get_collected_answer_row_index(&mut self, index: usize) -> &ConceptRow {
    //     self.unwrap_answer_if_needed().await;
    //     self.collected_rows.as_ref().unwrap().get(index).unwrap()
    // }
    //
    // pub async fn try_get_concurrent_rows_streams(
    //     &mut self,
    // ) -> Option<&mut Vec<BoxStream<'static, Result<(), HttpBehaviourTestError><ConceptRow>>>> {
    //     self.unwrap_concurrent_answers_into_rows_streams_if_neeed().await;
    //     self.concurrent_rows_streams.as_mut()
    // }

    // pub async fn get_concurrent_rows_streams(&mut self) -> &mut Vec<BoxStream<'static, Result<(), HttpBehaviourTestError><ConceptRow>>> {
    //     self.try_get_concurrent_rows_streams().await.unwrap()
    // }
}

impl Default for Context {
    fn default() -> Self {
        let transaction_options = TransactionOptions::default();
        let tls_root_ca = match std::env::var("ROOT_CA") {
            Ok(root_ca) => PathBuf::from(root_ca),
            Err(_) => PathBuf::new(),
        };
        Self {
            tls_root_ca,
            transaction_options,
            http_client: create_http_client(),
            transaction_ids: VecDeque::new(),
            answer: None,
            answer_type: None,
            answer_query_type: None,
            // collected_rows: None,
            collected_documents: None,
            concurrent_answers: Vec::new(),
            // concurrent_rows_streams: None,
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        println!("TODO: Implement! Cleanup the server");
    }
}

fn create_http_client() -> Client<HttpConnector> {
    Client::builder().build::<_, hyper::Body>(HttpConnector::new())
}

fn create_https_client() -> Client<HttpsConnector<HttpConnector>> {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots() // TODO: Use custom roots?
        .expect("Expected native roots")
        .https_only()
        .enable_http1()
        .build();
    Client::builder().build::<_, hyper::Body>(https)
}

macro_rules! in_background {
    ($context:ident, |$background:ident| $expr:expr) => {
        let $background = $context.create_http_client().await.unwrap();
        $expr
        $background.force_close().unwrap();
    };
}
pub(crate) use in_background;

#[derive(Debug, Clone)]
pub enum HttpBehaviourTestError {
    Transaction,
    InvalidConceptConversion,
    InvalidValueRetrieval(String),
}

impl fmt::Display for HttpBehaviourTestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transaction => write!(f, "Transaction error."),
            Self::InvalidConceptConversion => write!(f, "Invalid concept conversion."),
            Self::InvalidValueRetrieval(type_) => write!(f, "Could not retrieve a '{}' value.", type_),
        }
    }
}

impl Error for HttpBehaviourTestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transaction => None,
            Self::InvalidConceptConversion => None,
            Self::InvalidValueRetrieval(_) => None,
        }
    }
}

#[macro_export]
macro_rules! generic_step {
    {$(#[step($pattern:expr)])+ $vis:vis $async:ident fn $fn_name:ident $args:tt $(-> $res:ty)? $body:block} => {
        #[allow(unused)]
        $vis $async fn $fn_name $args $(-> $res)? $body

        const _: () = {
            $(
            #[::cucumber::given($pattern)]
            #[::cucumber::when($pattern)]
            #[::cucumber::then($pattern)]
            )+
            $vis $async fn step $args $(-> $res)? $body
        };
    };
}

#[macro_export]
macro_rules! assert_err {
    ($expr:expr) => {{
        let res = $expr;
        assert!(res.is_err(), "{res:?}")
    }};
}
