//! TLS client configuration shared by the sources that connect with TLS (`mqtts://` MQTT
//! sources and Kafka sources with `tls = true`).
//!
//! rustls with the aws-lc-rs provider (AES-GCM through the Arm AES/PMULL instructions, no
//! OpenSSL), TLS 1.3 and 1.2. The broker certificate is checked against the public (Mozilla)
//! roots, or, when [`TlsFiles::ca`] is given, against that CA only (a private PKI must not also
//! trust every public CA). [`TlsFiles::cert`] + [`TlsFiles::key`] add a client certificate
//! (mutual TLS). The provider is passed explicitly, so the configuration does not depend on a
//! process-wide default and is the same whatever crypto features a client library enables.

use std::sync::Arc;

use super::TlsFiles;

/// Build the TLS client configuration of a source. Reads the PEM files; every error names the
/// file and what is wrong with it.
pub fn client_config(files: &TlsFiles) -> Result<Arc<rustls::ClientConfig>, String> {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let read_certs = |path: &std::path::Path| -> Result<Vec<CertificateDer<'static>>, String> {
        let certs = CertificateDer::pem_file_iter(path)
            .map_err(|e| format!("{}: {e}", path.display()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if certs.is_empty() {
            return Err(format!(
                "{}: no PEM certificate in the file",
                path.display()
            ));
        }
        Ok(certs)
    };
    let mut roots = rustls::RootCertStore::empty();
    match &files.ca {
        Some(path) => {
            for c in read_certs(path)? {
                roots
                    .add(c)
                    .map_err(|e| format!("{}: not a usable CA certificate: {e}", path.display()))?;
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS: {e}"))?
        .with_root_certificates(roots);
    let config = match (&files.cert, &files.key) {
        (Some(cert), Some(key)) => {
            let chain = read_certs(cert)?;
            let key = PrivateKeyDer::from_pem_file(key)
                .map_err(|e| format!("{}: no usable PEM private key: {e}", key.display()))?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| format!("client certificate and key do not match: {e}"))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => return Err("tls_cert and tls_key must be given together".to_owned()),
    };
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TLS material is checked when the source starts: every bad input is refused with the file
    /// it came from; good inputs give a configuration.
    #[test]
    fn tls_files_are_checked() {
        let dir = tempfile::tempdir().unwrap();
        let w = |name: &str, body: &str| {
            let p = dir.path().join(name);
            std::fs::write(&p, body).unwrap();
            p
        };
        let ca = rcgen::generate_simple_self_signed(vec!["broker.plant".into()]).unwrap();
        let client = rcgen::generate_simple_self_signed(vec!["ankaa".into()]).unwrap();
        let other = rcgen::generate_simple_self_signed(vec!["other".into()]).unwrap();
        let ca_pem = w("ca.pem", &ca.cert.pem());
        let cert_pem = w("client.pem", &client.cert.pem());
        let key_pem = w("client.key", &client.signing_key.serialize_pem());
        let wrong_key = w("other.key", &other.signing_key.serialize_pem());
        let empty = w("empty.pem", "");
        // public roots, a private CA, a client certificate
        client_config(&TlsFiles::default()).unwrap();
        client_config(&TlsFiles {
            ca: Some(ca_pem.clone()),
            ..TlsFiles::default()
        })
        .unwrap();
        client_config(&TlsFiles {
            ca: Some(ca_pem.clone()),
            cert: Some(cert_pem.clone()),
            key: Some(key_pem.clone()),
        })
        .unwrap();
        // refused, each with a reason that names the problem
        let err = |f: TlsFiles| client_config(&f).unwrap_err();
        assert!(
            err(TlsFiles {
                ca: Some(dir.path().join("missing.pem")),
                ..TlsFiles::default()
            })
            .contains("missing.pem")
        );
        assert!(
            err(TlsFiles {
                ca: Some(empty.clone()),
                ..TlsFiles::default()
            })
            .contains("no PEM certificate")
        );
        assert!(
            err(TlsFiles {
                cert: Some(cert_pem.clone()),
                ..TlsFiles::default()
            })
            .contains("together")
        );
        assert!(
            err(TlsFiles {
                cert: Some(cert_pem.clone()),
                key: Some(empty),
                ..TlsFiles::default()
            })
            .contains("private key")
        );
        assert!(
            err(TlsFiles {
                cert: Some(cert_pem),
                key: Some(wrong_key),
                ..TlsFiles::default()
            })
            .contains("do not match")
        );
    }
}
