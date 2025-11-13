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

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Generates a self-signed certificate for the given domain.
/// Returns a tuple of (certificate chain, private key).
pub fn generate_self_signed_certificate(
    domain: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    let cert = rcgen::generate_simple_self_signed(vec![domain.to_string()])?;
    let cert_der = cert.cert.der();
    let key_der = cert.signing_key.serialize_der();
    let key = PrivateKeyDer::try_from(key_der)?;

    Ok((vec![cert_der.clone()], key))
}
