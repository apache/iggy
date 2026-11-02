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

namespace Apache.Iggy.Messages;

/// <summary>
///     Sizes of the canonical message batch record shared by send requests and poll responses:
///     a 256-byte batch header followed by per-message frames of
///     <c>[48-byte frame header][payload][user headers]</c>.
/// </summary>
internal static class BatchWireFormat
{
    internal const int BATCH_HEADER_SIZE = 256;

    internal const int FRAME_HEADER_SIZE = 48;
}
