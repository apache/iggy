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

namespace Apache.Iggy.Encryption;

/// <summary>
/// Interface for encrypting and decrypting message payloads in Iggy messaging system.
/// Implementations of this interface can be used with IggyPublisher and IggyConsumer
/// to provide end-to-end encryption of message data.
/// </summary>
public interface IMessageEncryptor
{
    /// <summary>
    /// Encrypts the provided plain data.
    /// </summary>
    /// <param name="plainData">The plain data to encrypt</param>
    /// <returns>The encrypted data</returns>
    byte[] Encrypt(byte[] plainData);

    /// <summary>
    /// Decrypts the provided encrypted data.
    /// </summary>
    /// <param name="encryptedData">The encrypted data to decrypt</param>
    /// <returns>The decrypted plain data</returns>
    byte[] Decrypt(byte[] encryptedData);
}
