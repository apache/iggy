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

// Partition sentinels a poll can carry instead of a real partition id.
const (
	// ResyncRequiredPartition is the partition id the server returns with a
	// zero message count to tell the member its assignment is stale.
	ResyncRequiredPartition uint32 = 0xFFFFFFFF
	// NoAssignedPartition is the partition id the SDK reports when the member
	// currently owns no partition of the group.
	NoAssignedPartition uint32 = 0xFFFFFFFE
)

// ConsumerGroupAssignment is the partitions a member owns at a generation.
// The generation advances on every rebalance, and a poll against a stale
// generation is fenced.
type ConsumerGroupAssignment struct {
	Generation uint64
	Partitions []uint32
}
