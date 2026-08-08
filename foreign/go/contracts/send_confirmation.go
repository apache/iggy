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

package iggcon

// SendMessagesConfirmation is the placement of one committed batch.
type SendMessagesConfirmation struct {
	StreamId    uint32
	TopicId     uint32
	PartitionId uint32
	// BaseOffset is the offset of the first message of the batch.
	BaseOffset uint64
}

// SendMessagesResponse is what the server confirmed for a send.
//
// Delivery is at-least-once. A retried send may have committed earlier at a
// lower offset, so the same messages can appear twice. A confirmation means
// the batch reached an in-memory commit, not that it was flushed to disk. An
// empty list is a valid success.
type SendMessagesResponse struct {
	Confirmations []SendMessagesConfirmation
}
