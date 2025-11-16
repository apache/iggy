// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

namespace Apache.Iggy.Contracts.Http;

/// <summary>
/// Represents an error response returned by a service or API.
/// Contains detailed information about the error that occurred.
/// </summary>
public class ErrorResponse
{
    /// <summary>
    /// Unique identifier associated with the error response.
    /// </summary>
    public int Id { get; set; }

    /// <summary>
    /// Error code that identifies the specific type of error encountered.
    /// </summary>
    public string? Code { get; set; }

    /// <summary>
    /// Provides a description of the cause of the error or failure.
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// Specifies the field associated with the error in the response.
    /// </summary>
    public string? Field { get; set; }
}
