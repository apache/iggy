use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::sync::Arc;

use tracing::error;
use iggy_common::{AutoLogin, IggyDuration, IggyError, TcpClientConfig};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConnection, StreamOwned};

pub trait ClientConfig {
    fn server_address(&self) -> SocketAddr;
    fn auto_login(&self) -> AutoLogin;
    fn reconnection_reestablish_after(&self) -> IggyDuration;
    fn reconnection_max_retries(&self) -> Option<u32>;
    fn reconnection_enabled(&self) -> bool;
    fn reconnection_interval(&self) -> IggyDuration;
    fn heartbeat_interval(&self) -> IggyDuration;
}

impl ClientConfig for TcpClientConfig {
    fn auto_login(&self) -> AutoLogin {
        self.auto_login.clone()
    }

    fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    fn reconnection_max_retries(&self) -> Option<u32> {
        self.reconnection.max_retries
    }

    fn reconnection_reestablish_after(&self) -> IggyDuration {
        self.reconnection.reestablish_after
    }

    fn reconnection_enabled(&self) -> bool {
        self.reconnection.enabled
    }

    fn reconnection_interval(&self) -> IggyDuration {
        self.reconnection.interval
    }

    fn server_address(&self) -> SocketAddr {
        SocketAddr::from_str(&self.server_address).unwrap()
    }
}

pub trait Transport {
    type Stream: Read + Write;
    type Config: ClientConfig;

    fn connect(
        cfg: Arc<Self::Config>, // TODO maybe should remove arc
        server_address: SocketAddr,
    ) -> Result<Self::Stream, IggyError>;
    
    fn shutdown(stream: &mut Self::Stream) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct TcpTransport;

impl Transport for TcpTransport {
    type Stream = TcpStream;
    type Config = TcpClientConfig;

    fn connect(
            cfg: Arc<Self::Config>,
            server_address: SocketAddr,
        ) -> Result<Self::Stream, IggyError> {
            let stream = TcpStream::connect(server_address).map_err(|e| {
                error!("Failed to establish a TCP connection to the server: {e}",);
                IggyError::CannotEstablishConnection
            })?;
            if let Err(e) = stream.set_nodelay(cfg.nodelay) {
                error!("Failed to set the nodelay option on the client: {e}, continuing...",);
            }
            Ok(stream)
        }
    
    fn shutdown(stream: &mut Self::Stream) -> Result<(), IggyError> {
        stream.shutdown(std::net::Shutdown::Both).map_err(|e| {
            error!(
                "Failed to shutdown the TCP connection to the TCP connection: {e}",
            );
            IggyError::TcpError
        })
    }
}

#[derive(Debug)]
pub struct TcpTlsTransport;

impl Transport for TcpTlsTransport {
    type Stream = StreamOwned<ClientConnection, TcpStream>;
    type Config = TcpClientConfig;

    fn connect(
            cfg: Arc<Self::Config>,
            server_address: SocketAddr,
        ) -> Result<Self::Stream, IggyError> {
            let stream = TcpStream::connect(server_address).map_err(|e| {
                error!("Failed to establish a TLS connection to the server: {e}",);
                IggyError::CannotEstablishConnection
            })?;
            if let Err(e) = stream.set_nodelay(cfg.nodelay) {
                error!("Failed to set the nodelay option on the client: {e}, continuing...",);
            }

            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

            let config = if cfg.tls_validate_certificate {
                let mut root_cert_store = rustls::RootCertStore::empty();
                if let Some(certificate_path) = cfg.tls_ca_file.clone() {
                    for cert in
                        CertificateDer::pem_file_iter(&certificate_path).map_err(|error| {
                            error!("Failed to read the CA file: {certificate_path}. {error}",);
                            IggyError::InvalidTlsCertificatePath
                        })?
                    {
                        let certificate = cert.map_err(|error| {
                            error!(
                                "Failed to read a certificate from the CA file: {certificate_path}. {error}",
                            );
                            IggyError::InvalidTlsCertificate
                        })?;
                        root_cert_store.add(certificate).map_err(|error| {
                            error!(
                                "Failed to add a certificate to the root certificate store. {error}",
                            );
                            IggyError::InvalidTlsCertificate
                        })?;
                    }
                } else {
                    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                }

                rustls::ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth()
            } else {
                use crate::tcp::tcp_tls_verifier::NoServerVerification;
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoServerVerification))
                    .with_no_client_auth()
            };

            let tls_domain = cfg.tls_domain.to_owned();
            let domain = ServerName::try_from(tls_domain).map_err(|error| {
                error!("Failed to create a server name from the domain. {error}",);
                IggyError::InvalidTlsDomain
            })?;
            

            let conn = ClientConnection::new(Arc::new(config), domain)
                .map_err(|e| {
                    error!("Failed to establish a TCP/TLS connection to the server: {e}",);
                    IggyError::CannotEstablishConnection
                })?;

            Ok(StreamOwned::new(conn, stream))
    }

    fn shutdown(stream: &mut Self::Stream) -> Result<(), IggyError> {
        stream.conn.send_close_notify();
        stream.sock.shutdown(std::net::Shutdown::Both).map_err(|e| {
            error!(
                "Failed to shutdown the TCP/TLS connection to the TCP connection: {e}",
            );
            IggyError::TcpError
        })
    }
}
