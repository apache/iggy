// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, AeadCore, Generate, KeyInit, Nonce};
use aes_gcm_siv::Aes256GcmSiv;
use iggy_common::{Aes256GcmEncryptor, Aes256GcmSivEncryptor, Encryptor};
use std::hint::black_box;
use std::time::Instant;
use xaes_256_gcm::Xaes256Gcm;

fn measure<C>(name: &str, plaintext: &[u8], iterations: usize)
where
    C: Aead + AeadCore + KeyInit,
    Nonce<C>: Generate,
{
    let cipher = C::new_from_slice(&[0x5a; 32]).expect("valid key");
    let sample_nonce = Nonce::<C>::generate();
    let sample = cipher
        .encrypt(&sample_nonce, plaintext)
        .expect("sample encrypts");
    assert_eq!(
        cipher
            .decrypt(&sample_nonce, sample.as_ref())
            .expect("sample decrypts"),
        plaintext
    );

    let start = Instant::now();
    for _ in 0..iterations {
        let nonce = Nonce::<C>::generate();
        black_box(
            cipher
                .encrypt(&nonce, black_box(plaintext))
                .expect("encrypts"),
        );
    }
    let encrypt = start.elapsed().as_nanos() as f64 / iterations as f64;

    let start = Instant::now();
    for _ in 0..iterations {
        black_box(
            cipher
                .decrypt(&sample_nonce, black_box(sample.as_ref()))
                .expect("decrypts"),
        );
    }
    let decrypt = start.elapsed().as_nanos() as f64 / iterations as f64;

    println!("{name}, {}, {encrypt:.1}, {decrypt:.1}", plaintext.len());
}

fn measure_wrapper<E: Encryptor>(name: &str, cipher: &E, plaintext: &[u8], iterations: usize) {
    let sample = cipher.encrypt(plaintext).expect("sample encrypts");
    assert_eq!(cipher.decrypt(&sample).expect("sample decrypts"), plaintext);

    let start = Instant::now();
    for _ in 0..iterations {
        black_box(cipher.encrypt(black_box(plaintext)).expect("encrypts"));
    }
    let encrypt = start.elapsed().as_nanos() as f64 / iterations as f64;

    let start = Instant::now();
    for _ in 0..iterations {
        black_box(cipher.decrypt(black_box(&sample)).expect("decrypts"));
    }
    let decrypt = start.elapsed().as_nanos() as f64 / iterations as f64;

    println!("{name}, {}, {encrypt:.1}, {decrypt:.1}", plaintext.len());
}

fn main() {
    println!("cipher, bytes, encrypt_ns_per_op, decrypt_ns_per_op");
    let current = Aes256GcmEncryptor::new(&[0x5a; 32]).expect("valid key");
    let siv = Aes256GcmSivEncryptor::new(&[0x5a; 32]).expect("valid key");
    for (size, iterations) in [
        (64, 100_000),
        (1024, 50_000),
        (16 * 1024, 5_000),
        (1024 * 1024, 200),
    ] {
        let plaintext = vec![0x42; size];
        measure::<Aes256Gcm>("AES-256-GCM", &plaintext, iterations);
        measure::<Aes256GcmSiv>("AES-256-GCM-SIV", &plaintext, iterations);
        measure::<Xaes256Gcm>("XAES-256-GCM", &plaintext, iterations);
        measure_wrapper("current-wrapper", &current, &plaintext, iterations);
        measure_wrapper("siv-wrapper", &siv, &plaintext, iterations);
    }
}
