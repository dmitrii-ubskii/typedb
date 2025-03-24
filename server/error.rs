/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{io, net::SocketAddr, sync::Arc};

use database::DatabaseOpenError;
use error::typedb_error;
use tokio_rustls::rustls::{
    pki_types::pem::Error as RustlsCertError, server::VerifierBuilderError as RustlsVerifierError,
};

typedb_error! {
    pub ServerOpenError(component = "Server open", prefix = "SRO") {
        NotADirectory(1, "Invalid path '{path}': not a directory.", path: String),
        CouldNotReadServerIDFile(2, "Could not read data from server ID file '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotCreateServerIDFile(3, "Could not write data to server ID file '{path}'.", path: String, source: Arc<io::Error>),
        CouldNotCreateDataDirectory(4, "Could not create data directory in '{path}'.", path: String, source: Arc<io::Error>),
        InvalidServerID(5, "Server ID read from '{path}' is invalid. Delete the corrupted file and try again.", path: String),
        DatabaseOpen(6, "Could not open database.", typedb_source: DatabaseOpenError),
        MissingTLSCertificate(7, "TLS certificate path must be specified when encryption is enabled."),
        MissingTLSCertificateKey(8, "TLS certificate key path must be specified when encryption is enabled."),
        GPRCHTTPConflictingAddress(9, "Configuring HTTP and gRPC on the same address {address} is not supported.", address: SocketAddr),
        GRPCServe(10, "Could not serve gRPC on {address}.", address: SocketAddr, source: Arc<tonic::transport::Error>),
        GRPCCouldNotReadTLSCertificate(11, "Could not read TLS certificate from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GRPCCouldNotReadTLSCertificateKey(12, "Could not read TLS certificate key from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GRPCCouldNotReadRootCA(13, "Could not read root CA from '{path}' for the gRPC server.", path: String, source: Arc<io::Error>),
        GRPCInvalidRootCA(14, "Invalid root CA for the gRPC server.", source: Arc<io::Error>),
        GRPCTLSFailedConfiguration(15, "Failed to configure TLS for the gRPC server.", source: Arc<tonic::transport::Error>),
        HTTPServe(16, "Could not serve HTTP on {address}.", address: SocketAddr, source: Arc<io::Error>),
        HTTPCouldNotReadTLSCertificate(17, "Could not read TLS certificate from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HTTPCouldNotReadTLSCertificateKey(18, "Could not read TLS certificate key from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HTTPCouldNotReadRootCA(19, "Could not read root CA from '{path}' for the HTTP server.", path: String, source: Arc<RustlsCertError>),
        HTTPInvalidRootCA(20, "Invalid root CA for the HTTP server.", source: Arc<RustlsVerifierError>),
        HTTPTLSFailedConfiguration(21, "Failed to configure TLS for the HTTP server.", source: Arc<tokio_rustls::rustls::Error>),
        HTTPTLSUnsetDefaultCryptoProvider(22, "Failed to install default crypto provider for the HTTP server TLS configuration."),
    }
}
