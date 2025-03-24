/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use itertools::Itertools;
pub(crate) use tokio_rustls::rustls::ServerConfig as HTTPTLSConfig;
use tokio_rustls::rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    RootCertStore,
};

use crate::{error::ServerOpenError, parameters::config::EncryptionConfig};

pub(crate) fn prepare_tls_config(
    encryption_config: &EncryptionConfig,
) -> Result<Option<HTTPTLSConfig>, ServerOpenError> {
    if !encryption_config.enabled {
        return Ok(None);
    }

    let cert_path = encryption_config.cert.as_ref().ok_or(ServerOpenError::MissingTLSCertificate {})?;
    let cert_iter = CertificateDer::pem_file_iter(cert_path.as_path()).map_err(|source| {
        ServerOpenError::HTTPCouldNotReadTLSCertificate {
            path: cert_path.display().to_string(),
            source: Arc::new(source),
        }
    })?;
    let certs: Vec<_> = cert_iter.try_collect().map_err(|source| ServerOpenError::HTTPCouldNotReadTLSCertificate {
        path: cert_path.display().to_string(),
        source: Arc::new(source),
    })?;

    let cert_key_path = encryption_config.cert_key.as_ref().ok_or(ServerOpenError::MissingTLSCertificateKey {})?;
    let key = PrivateKeyDer::from_pem_file(cert_key_path.as_path()).map_err(|source| {
        ServerOpenError::HTTPCouldNotReadTLSCertificateKey {
            path: cert_key_path.display().to_string(),
            source: Arc::new(source),
        }
    })?;

    let client_cert_verifier = match &encryption_config.root_ca {
        Some(root_ca_path) => {
            let mut client_auth_roots = RootCertStore::empty();
            let mut root_ca_iter = CertificateDer::pem_file_iter(root_ca_path.as_path()).map_err(|source| {
                ServerOpenError::HTTPCouldNotReadRootCA {
                    path: root_ca_path.display().to_string(),
                    source: Arc::new(source),
                }
            })?;
            while let Some(root_ca) = root_ca_iter.next() {
                client_auth_roots.add(root_ca.unwrap()).unwrap(); // TODO: Convert to errors
            }
            Some(
                WebPkiClientVerifier::builder(Arc::new(client_auth_roots))
                    .build()
                    .map_err(|source| ServerOpenError::HTTPInvalidRootCA { source: Arc::new(source) })?,
            )
        }
        None => None,
    };

    let config_builder = match client_cert_verifier {
        Some(client_cert_verifier) => HTTPTLSConfig::builder().with_client_cert_verifier(client_cert_verifier),
        None => HTTPTLSConfig::builder().with_no_client_auth(),
    };

    let config = config_builder
        .with_single_cert(certs, key)
        .map_err(|source| ServerOpenError::HTTPTLSFailedConfiguration { source: Arc::new(source) })?;

    Ok(Some(config))
}
