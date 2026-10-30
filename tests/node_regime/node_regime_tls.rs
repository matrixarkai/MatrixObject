// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Ephemeral certificates; no private key material is stored in the repository.
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct TlsFiles {
    pub root: PathBuf,
}
impl TlsFiles {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "matrixobject-tls-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let result = Self { root };
        result.run(&[
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "2",
            "-subj",
            "/CN=local-peer-test-ca",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]);
        result.issue("peer", "127.0.0.1");
        result
    }
    pub fn issue(&self, name: &str, ip: &str) {
        let key = format!("{name}.key");
        let csr = format!("{name}.csr");
        let cert = format!("{name}.pem");
        let extensions = format!("{name}.extensions");
        std::fs::write(self.root.join(&extensions), format!("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=IP:{ip}\n")).unwrap();
        self.run(&[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-keyout",
            &key,
            "-out",
            &csr,
            "-subj",
            "/CN=local-peer-test",
        ]);
        self.run(&[
            "x509",
            "-req",
            "-in",
            &csr,
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            &cert,
            "-days",
            "2",
            "-extfile",
            &extensions,
        ]);
    }
    fn run(&self, args: &[&str]) {
        let result = Command::new("openssl")
            .args(args)
            .current_dir(&self.root)
            .output()
            .expect("openssl is required for local TLS tests");
        assert!(
            result.status.success(),
            "certificate generation failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    pub fn path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }
}
impl Drop for TlsFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
