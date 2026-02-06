#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Generate JWKS JSON from RSA public key."""

import base64
import json
import subprocess
import sys


def hex_to_base64url(hex_string):
    """Convert hex string to Base64 URL-safe encoding."""
    hex_string = hex_string.strip()
    bytes_data = bytes.fromhex(hex_string)
    return base64.urlsafe_b64encode(bytes_data).decode('ascii').rstrip('=')


def get_modulus_from_pem(pem_file):
    """Extract modulus from RSA public key PEM file."""
    result = subprocess.run(
        ['openssl', 'rsa', '-pubin', '-in', pem_file, '-noout', '-modulus'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    output = result.stdout.strip()
    if '=' in output:
        return output.split('=')[1]
    return output


def main():
    pem_file = '/Users/yanglx/Desktop/iggy/core/integration/tests/server/a2a_jwt/test_public_key.pem'

    modulus_hex = get_modulus_from_pem(pem_file)
    modulus_b64 = hex_to_base64url(modulus_hex)

    # RSA exponent is typically 65537 = 0x10001 = AQAB in Base64
    exponent_b64 = "AQAB"

    jwks = {
        "keys": [
            {
                "kty": "RSA",
                "kid": "test-key-1",
                "n": modulus_b64,
                "e": exponent_b64
            }
        ]
    }

    print(json.dumps(jwks, indent=2))


if __name__ == "__main__":
    main()
