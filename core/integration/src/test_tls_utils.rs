/* Licensed to the Apache Software Foundation (ASF) under one
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

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

/// Generate self-signed certificates for TLS testing
pub fn generate_test_certificates(cert_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(cert_dir)?;

    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];

    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;

    let cert_path = Path::new(cert_dir).join("test_cert.pem");
    let mut cert_file = File::create(&cert_path)?;
    cert_file.write_all(cert.cert.pem().as_bytes())?;

    let key_path = Path::new(cert_dir).join("test_key.pem");
    let mut key_file = File::create(&key_path)?;
    key_file.write_all(cert.signing_key.serialize_pem().as_bytes())?;

    Ok(())
}

/// Clean up test certificates
pub fn cleanup_test_certificates(cert_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    if Path::new(cert_dir).exists() {
        fs::remove_dir_all(cert_dir)?;
    }
    Ok(())
}
