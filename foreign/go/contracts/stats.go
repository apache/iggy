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

type Stats struct {
	ProcessId           int     `json:"process_id"`
	CpuUsage            float32 `json:"cpu_usage"`
	TotalCpuUsage       float32 `json:"total_cpu_usage"`
	MemoryUsage         uint64  `json:"memory_usage"`
	TotalMemory         uint64  `json:"total_memory"`
	AvailableMemory     uint64  `json:"available_memory"`
	RunTime             uint64  `json:"run_time"`
	StartTime           uint64  `json:"start_time"`
	ReadBytes           uint64  `json:"read_bytes"`
	WrittenBytes        uint64  `json:"written_bytes"`
	MessagesSizeBytes   uint64  `json:"messages_size_bytes"`
	StreamsCount        int     `json:"streams_count"`
	TopicsCount         int     `json:"topics_count"`
	PartitionsCount     int     `json:"partitions_count"`
	SegmentsCount       int     `json:"segments_count"`
	MessagesCount       uint64  `json:"messages_count"`
	ClientsCount        int     `json:"clients_count"`
	ConsumerGroupsCount int     `json:"consumer_groups_count"`
	Hostname            string  `json:"hostname"`
	OsName              string  `json:"os_name"`
	OsVersion           string  `json:"os_version"`
	KernelVersion       string  `json:"kernel_version"`
}
