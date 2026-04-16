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

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/apache/iggy/foreign/go/internal/codec"
	"github.com/google/go-cmp/cmp"
)

// buildStatsPayload constructs a wire payload for Stats using the same
// layout as the Rust binary_protocol StatsResponse encoder.
func buildStatsPayload(t *testing.T, s Stats) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.U32(s.ProcessId)
	w.F32(s.CpuUsage)
	w.F32(s.TotalCpuUsage)
	w.U64(s.MemoryUsage)
	w.U64(s.TotalMemory)
	w.U64(s.AvailableMemory)
	w.U64(s.RunTime)
	w.U64(s.StartTime)
	w.U64(s.ReadBytes)
	w.U64(s.WrittenBytes)
	w.U64(s.MessagesSizeBytes)
	w.U32(s.StreamsCount)
	w.U32(s.TopicsCount)
	w.U32(s.PartitionsCount)
	w.U32(s.SegmentsCount)
	w.U64(s.MessagesCount)
	w.U32(s.ClientsCount)
	w.U32(s.ConsumerGroupsCount)
	w.U32LenStr(s.Hostname)
	w.U32LenStr(s.OsName)
	w.U32LenStr(s.OsVersion)
	w.U32LenStr(s.KernelVersion)
	w.U32LenStr(s.IggyServerVersion)

	w.U32(s.IggyServerSemver)

	w.U32(uint32(len(s.CacheMetrics)))
	for i := range s.CacheMetrics {
		w.Obj(&s.CacheMetrics[i])
	}

	w.U32(s.ThreadsCount)
	w.U64(s.FreeDiskSpace)
	w.U64(s.TotalDiskSpace)

	if err := w.Err(); err != nil {
		t.Fatalf("buildStatsPayload: %v", err)
	}
	return w.Bytes()
}

func sampleStats() Stats {
	return Stats{
		ProcessId:           1234,
		CpuUsage:            25.5,
		TotalCpuUsage:       50.0,
		MemoryUsage:         1_073_741_824,
		TotalMemory:         8_589_934_592,
		AvailableMemory:     4_294_967_296,
		RunTime:             3600,
		StartTime:           1_710_000_000_000,
		ReadBytes:           1_000_000,
		WrittenBytes:        500_000,
		MessagesSizeBytes:   2_000_000,
		StreamsCount:        3,
		TopicsCount:         10,
		PartitionsCount:     30,
		SegmentsCount:       90,
		MessagesCount:       50_000,
		ClientsCount:        5,
		ConsumerGroupsCount: 2,
		Hostname:            "node-1",
		OsName:              "Linux",
		OsVersion:           "6.1",
		KernelVersion:       "6.1.0",
		IggyServerVersion:   "0.6.0",
		IggyServerSemver:    600,
		ThreadsCount:        16,
		FreeDiskSpace:       107_374_182_400,
		TotalDiskSpace:      512_110_190_592,
	}
}

func assertStatsEqual(t *testing.T, got, want Stats) {
	t.Helper()

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Stats mismatch (-want +got):\n%s", diff)
	}
}

func TestUnmarshalBinary(t *testing.T) {
	want := sampleStats()
	payload := buildStatsPayload(t, want)

	var got Stats
	if err := got.UnmarshalBinary(payload); err != nil {
		t.Fatalf("UnmarshalBinary error: %v", err)
	}

	assertStatsEqual(t, got, want)
}

func TestUnmarshalBinary_maliciousCacheCount(t *testing.T) {
	s := Stats{
		Hostname:          "h",
		OsName:            "o",
		OsVersion:         "s",
		KernelVersion:     "t",
		IggyServerVersion: "v",
	}
	payload := buildStatsPayload(t, s)
	// cache_metrics_count sits before the tail fields: threads(4) + free_disk(8) + total_disk(8) = 20
	cacheCountOffset := len(payload) - 20 - 4
	binary.LittleEndian.PutUint32(payload[cacheCountOffset:], math.MaxUint32)

	var stats Stats
	err := stats.UnmarshalBinary(payload)
	if err == nil {
		t.Fatal("expected error for oversized cache metrics count, got nil")
	}
	if !strings.Contains(err.Error(), "cache metrics count") {
		t.Errorf("unexpected error: got %v, want error mentioning cache metrics count", err)
	}
}

func TestUnmarshalBinary_withCacheMetrics(t *testing.T) {
	want := sampleStats()
	want.CacheMetrics = []CacheMetrics{
		{StreamId: 1, TopicId: 1, PartitionId: 0, Hits: 1000, Misses: 50, HitRatio: 0.95238095},
		{StreamId: 2, TopicId: 3, PartitionId: 1, Hits: 0, Misses: 100, HitRatio: 0.0},
	}
	payload := buildStatsPayload(t, want)

	var got Stats
	if err := got.UnmarshalBinary(payload); err != nil {
		t.Fatalf("UnmarshalBinary error: %v", err)
	}

	assertStatsEqual(t, got, want)
}

func TestUnmarshalBinary_truncatedFixedFields(t *testing.T) {
	// Payload too short to contain all fixed fields.
	payload := make([]byte, 10) // far too small
	var stats Stats
	if err := stats.UnmarshalBinary(payload); err == nil {
		t.Fatal("expected error for truncated payload, got nil")
	}
}

func TestUnmarshalBinary_truncatedAfterStrings(t *testing.T) {
	// Build a valid payload then chop off everything after the semver
	// so that reading cache count fails.
	s := Stats{
		Hostname:          "h",
		OsName:            "o",
		OsVersion:         "v",
		KernelVersion:     "k",
		IggyServerVersion: "s",
	}
	payload := buildStatsPayload(t, s)
	// Remove tail fields + cache_metrics_count (4+4+8+8 = 24 bytes from the end).
	payload = payload[:len(payload)-24]

	var stats Stats
	if err := stats.UnmarshalBinary(payload); err == nil {
		t.Fatal("expected error for truncated cache count, got nil")
	}
}

func TestUnmarshalBinary_emptyStrings(t *testing.T) {
	want := sampleStats()
	want.Hostname = ""
	want.OsName = ""
	want.OsVersion = ""
	want.KernelVersion = ""
	want.IggyServerVersion = ""
	payload := buildStatsPayload(t, want)

	var got Stats
	if err := got.UnmarshalBinary(payload); err != nil {
		t.Fatalf("UnmarshalBinary error: %v", err)
	}

	assertStatsEqual(t, got, want)
}
