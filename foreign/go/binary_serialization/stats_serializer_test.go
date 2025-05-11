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

package binaryserialization

import (
	"testing"
)

func TestDeserialize(t *testing.T) {
	payload2 := []byte{
		240, 68, 0, 0, //"process_id": 17648,
		124, 202, 146, 58, //"cpu_usage": 0.0011199261,
		0, 224, 66, 3, 0, 0, 0, 0, //"memory_usage": 54714368,
		0, 32, 123, 208, 7, 0, 0, 0, //"total_memory": 33562501120,
		0, 224, 28, 253, 4, 0, 0, 0, //"available_memory": 21426397184,
		169, 13, 0, 0, 0, 0, 0, 0, //"run_time": 3497,
		239, 201, 6, 101, 0, 0, 0, 0, //"start_time": 1694943727,
		0, 0, 0, 0, 0, 0, 0, 0, //"read_bytes": 0,
		0, 128, 8, 0, 0, 0, 0, 0, //"written_bytes": 557056,
		138, 2, 0, 0, 0, 0, 0, 0, //"messages_size_bytes": 650,
		1, 0, 0, 0, //"streams_count": 1,
		1, 0, 0, 0, //"topics_count": 1,
		12, 0, 0, 0, //"partitions_count": 12,
		12, 0, 0, 0, //"segments_count": 12,
		4, 0, 0, 0, 0, 0, 0, 0, //"messages_count": 4,
		11, 0, 0, 0, //"clients_count": 11,
		0, 0, 0, 0, //"consumer_groups_count": 0,
		6, 0, 0, 0, //hostname length: 6
		112, 111, 112, 45, 111, 115, //"hostname": "pop-os",
		7, 0, 0, 0, //os name length :7
		80, 111, 112, 33, 95, 79, 83, //"os_name": "Pop!_OS",
		19, 0, 0, 0, //os version length: 19
		76, 105, 110, 117, 120, 32, 50, 50, 46, 48, 52, 32, 80, 111, 112, 33, 95, 79, 83, //"os_version": "Linux 22.04 Pop!_OS",
		22, 0, 0, 0, //kernel version length: 22
		54, 46, 52, 46, 54, 45, 55, 54, 48, 54, 48, 52, 48, 54, 45, 103, 101, 110, 101, 114, 105, 99, //"kernel_version": "6.4.6-76060406-generic"
	}

	// Create a TcpStats object and deserialize the payload
	var stats TcpStats
	err := stats.Deserialize(payload2)

	// Check if there was an error during deserialization
	if err != nil {
		t.Errorf("Deserialization error: %v", err)
	}

	// Verify the deserialized values
	if stats.ProcessId != 17648 {
		t.Errorf("ProcessId is incorrect. Expected: 17648, Got: %d", stats.ProcessId)
	}
	if stats.CpuUsage != 0.0011199261 {
		t.Errorf("CPUUsage is incorrect. Expected: 0.0011199261, Got: %f", stats.CpuUsage)
	}
	if stats.MemoryUsage != 54714368 {
		t.Errorf("MemoryUsage is incorrect. Expected: 54714368, Got: %d", stats.MemoryUsage)
	}
	if stats.TotalMemory != 33562501120 {
		t.Errorf("TotalMemory is incorrect. Expected: 33562501120, Got: %d", stats.TotalMemory)
	}
	if stats.AvailableMemory != 21426397184 {
		t.Errorf("AvailableMemory is incorrect. Expected: 21426397184, Got: %d", stats.AvailableMemory)
	}
	if stats.RunTime != 3497 {
		t.Errorf("RunTime is incorrect. Expected: 3497, Got: %d", stats.RunTime)
	}
	if stats.StartTime != 1694943727 {
		t.Errorf("StartTime is incorrect. Expected: 1694943727, Got: %d", stats.StartTime)
	}
	if stats.ReadBytes != 0 {
		t.Errorf("ReadBytes is incorrect. Expected: 0, Got: %d", stats.ReadBytes)
	}
	if stats.WrittenBytes != 557056 {
		t.Errorf("WrittenBytes is incorrect. Expected: 557056, Got: %d", stats.WrittenBytes)
	}
	if stats.MessagesSizeBytes != 650 {
		t.Errorf("MessagesSizeBytes is incorrect. Expected: 650, Got: %d", stats.MessagesSizeBytes)
	}
	if stats.StreamsCount != 1 {
		t.Errorf("StreamsCount is incorrect. Expected: 1, Got: %d", stats.StreamsCount)
	}
	if stats.TopicsCount != 1 {
		t.Errorf("TopicsCount is incorrect. Expected: 1, Got: %d", stats.TopicsCount)
	}
	if stats.PartitionsCount != 12 {
		t.Errorf("PartitionsCount is incorrect. Expected: 12, Got: %d", stats.PartitionsCount)
	}
	if stats.SegmentsCount != 12 {
		t.Errorf("SegmentsCount is incorrect. Expected: 12, Got: %d", stats.SegmentsCount)
	}
	if stats.MessagesCount != 4 {
		t.Errorf("MessagesCount is incorrect. Expected: 4, Got: %d", stats.MessagesCount)
	}
	if stats.ClientsCount != 11 {
		t.Errorf("ClientsCount is incorrect. Expected: 11, Got: %d", stats.ClientsCount)
	}
	if stats.ConsumerGroupsCount != 0 {
		t.Errorf("ConsumerGroupsCount is incorrect. Expected: 0, Got: %d", stats.ConsumerGroupsCount)
	}
	if stats.Hostname != "pop-os" {
		t.Errorf("Hostname is incorrect. Expected: \"pop-os\", Got: \"%s\"", stats.Hostname)
	}
	if stats.OsName != "Pop!_OS" {
		t.Errorf("OsName is incorrect. Expected: \"Pop!_OS\", Got: \"%s\"", stats.OsName)
	}
	if stats.OsVersion != "Linux 22.04 Pop!_OS" {
		t.Errorf("OsVersion is incorrect. Expected: \"Linux 22.04 Pop!_OS\", Got: \"%s\"", stats.OsVersion)
	}
	if stats.KernelVersion != "6.4.6-76060406-generic" {
		t.Errorf("KernelVersion is incorrect. Expected: \"6.4.6-76060406-generic\", Got: \"%s\"", stats.KernelVersion)
	}
}
