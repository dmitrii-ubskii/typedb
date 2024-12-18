/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cell::RefCell,
    collections::HashMap,
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Timelike, Utc};
use concurrency::{IntervalRunner, TokioIntervalRunner};
use hyper::{
    header::{HeaderValue, CONNECTION, CONTENT_TYPE},
    Body, Client, Method, Request,
};
use hyper_rustls::HttpsConnectorBuilder;
use logger::{debug, trace};
use resource::constants::{
    common::SECONDS_IN_MINUTE,
    diagnostics::{DISABLED_REPORTING_FILE_NAME, REPORT_INTERVAL, REPORT_ONCE_DELAY},
};

use crate::{hash_string_consistently, Diagnostics};

#[derive(Debug)]
pub struct Reporter {
    deployment_id: String,
    diagnostics: Arc<Diagnostics>,
    reporting_uri: &'static str,
    data_directory: PathBuf,
    is_enabled: bool,
    _reporting_job: Arc<Mutex<Option<TokioIntervalRunner>>>,
}

impl Reporter {
    pub(crate) fn new(
        deployment_id: String,
        diagnostics: Arc<Diagnostics>,
        reporting_uri: &'static str,
        data_directory: PathBuf,
        is_enabled: bool,
    ) -> Self {
        Self {
            deployment_id,
            diagnostics,
            reporting_uri,
            data_directory,
            is_enabled,
            _reporting_job: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn may_start(&self) {
        if self.is_enabled {
            Self::delete_disabled_reporting_file_if_exists(&self.data_directory);
            self.schedule_reporting().await;
        } else {
            self.report_once_if_needed();
        }
    }

    async fn schedule_reporting(&self) {
        let diagnostics = self.diagnostics.clone();
        let reporting_uri = self.reporting_uri;

        let reporting_job = TokioIntervalRunner::new_with_initial_delay(
            move || {
                let diagnostics = diagnostics.clone();
                async move {
                    Self::report(diagnostics, reporting_uri).await;
                }
            },
            REPORT_INTERVAL,
            self.calculate_initial_delay(),
        );
        *self._reporting_job.lock().expect("Expected reporting job exclusive lock acquisition") = Some(reporting_job);
    }

    fn report_once_if_needed(&self) {
        let disabled_reporting_file = self.data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if !disabled_reporting_file.exists() {
            let diagnostics = self.diagnostics.clone();
            let reporting_uri = self.reporting_uri;
            let data_directory = self.data_directory.clone();

            tokio::spawn(async move {
                tokio::time::sleep(REPORT_ONCE_DELAY).await;
                if Self::report(diagnostics, reporting_uri).await {
                    Self::save_disabled_reporting_file(&data_directory);
                }
            });
        }
    }

    fn calculate_initial_delay(&self) -> Duration {
        let report_interval_secs = REPORT_INTERVAL.as_secs();
        assert!(report_interval_secs == 3600, "Modify the algorithm if you change the interval!");

        let current_minute = Utc::now().minute() as u64;
        let scheduled_minute =
            hash_string_consistently(&self.deployment_id) % (report_interval_secs / SECONDS_IN_MINUTE);

        let delay_secs = if current_minute > scheduled_minute {
            report_interval_secs - (current_minute + scheduled_minute) * SECONDS_IN_MINUTE
        } else {
            (scheduled_minute - current_minute) * SECONDS_IN_MINUTE
        };
        Duration::from_secs(delay_secs)
    }

    async fn report(diagnostics: Arc<Diagnostics>, reporting_uri: &'static str) -> bool {
        let diagnostics_json = diagnostics.to_reporting_json_against_snapshot().to_string();
        diagnostics.take_snapshot();

        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("No native root CA certificates found")
            .https_only()
            .enable_http1()
            .build();
        let client = Client::builder().build::<_, Body>(https);
        let request = hyper::Request::post(reporting_uri)
            .header(CONTENT_TYPE, "application/json")
            .header(CONNECTION, "close")
            .body(Body::from(diagnostics_json))
            .expect("Failed to construct the request");

        match client.request(request).await {
            Ok(response) => {
                if response.status().is_success() {
                    true
                } else {
                    trace!("Failed to push diagnostics to {}: {}", reporting_uri, response.status());
                    false
                }
            }
            Err(e) => {
                trace!("Failed to push diagnostics to {}: {}", reporting_uri, e);
                false
            }
        }
    }

    fn save_disabled_reporting_file(data_directory: &PathBuf) {
        let disabled_reporting_file = data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if let Err(e) = fs::write(&disabled_reporting_file, Utc::now().to_string()) {
            debug!("Failed to save disabled reporting file: {}", e);
        }
    }

    fn delete_disabled_reporting_file_if_exists(data_directory: &PathBuf) {
        let disabled_reporting_file = data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if let Err(e) = fs::remove_file(&disabled_reporting_file) {
            debug!("Failed to delete disabled reporting file: {}", e);
        }
    }
}
