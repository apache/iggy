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

package vsr

import (
	"encoding/binary"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

// Namespace packing limits, a port of
// core/binary_protocol/src/namespace.rs.
const (
	MaxStreams    = 4096
	MaxTopics     = 4096
	MaxPartitions = 1_000_000

	streamBits    = 12
	topicBits     = 12
	partitionBits = 20

	partitionShift = 0
	topicShift     = partitionShift + partitionBits
	streamShift    = topicShift + topicBits
)

// MetadataConsensusNamespace routes a control-plane request to the metadata
// replica on shard 0. Plain 0 falls into namespace hashing and can land a
// Register on a peer shard that has no consensus instance.
const MetadataConsensusNamespace uint64 = 1 << 63

// Identifier kinds in the wire prefix [kind u8][len u8][value].
const (
	identifierKindNumeric = 1
	identifierKindString  = 2
)

// partitioningPartitionID is the only Partitioning kind that carries an
// explicit partition, which is what routing needs.
const partitioningPartitionID = 2

// PackNamespace packs stream, topic and partition ids into a routing
// namespace.
func PackNamespace(streamID, topicID, partitionID uint32) (uint64, error) {
	if err := validateNamespaceField(streamID, MaxStreams); err != nil {
		return 0, err
	}
	if err := validateNamespaceField(topicID, MaxTopics); err != nil {
		return 0, err
	}
	if err := validateNamespaceField(partitionID, MaxPartitions); err != nil {
		return 0, err
	}
	return uint64(streamID)<<streamShift |
		uint64(topicID)<<topicShift |
		uint64(partitionID)<<partitionShift, nil
}

// NamespaceForRequest selects the routing namespace for a request.
// Partition-plane commands derive it by peeking their own encoded payload. A
// named stream or topic identifier yields 0 so the server resolves the name.
func NamespaceForRequest(code uint32, payload []byte, operation Operation) (uint64, error) {
	if operation == OperationRegister || operation == OperationLogout {
		return MetadataConsensusNamespace, nil
	}
	if operation == OperationNonReplicated || IsMetadata(operation) {
		return 0, nil
	}

	switch command.Code(code) {
	case command.SendMessagesCode:
		return namespaceFromSendMessages(payload)
	case command.StoreOffsetCode, command.DeleteConsumerOffsetCode,
		command.StoreOffset2Code, command.DeleteConsumerOffset2Code:
		return namespaceFromConsumerOffset(payload)
	case command.DeleteSegmentsCode:
		return namespaceFromDeleteSegments(payload)
	default:
		// The guard that keeps a partition-classified operation this SDK
		// cannot peek from reaching the wire with a wrong namespace.
		return 0, ierror.ErrFeatureUnavailable
	}
}

// peekedIdentifier is a decoded identifier prefix. numeric is zero and named
// is true for a string identifier, which the server resolves.
type peekedIdentifier struct {
	numeric uint32
	named   bool
	length  int
}

func peekIdentifier(payload []byte, offset int) (peekedIdentifier, error) {
	if len(payload) < offset+2 {
		return peekedIdentifier{}, ierror.ErrInvalidCommand
	}
	kind := payload[offset]
	length := int(payload[offset+1])
	if len(payload) < offset+2+length {
		return peekedIdentifier{}, ierror.ErrInvalidCommand
	}
	switch {
	case kind == identifierKindNumeric && length == 4:
		return peekedIdentifier{
			numeric: binary.LittleEndian.Uint32(payload[offset+2:]),
			length:  2 + length,
		}, nil
	case kind == identifierKindString && length > 0:
		return peekedIdentifier{named: true, length: 2 + length}, nil
	default:
		return peekedIdentifier{}, ierror.ErrInvalidCommand
	}
}

func validateNamespaceField(value uint32, exclusiveMax uint32) error {
	if value >= exclusiveMax {
		return ierror.ErrInvalidIdentifier
	}
	return nil
}

func namespaceFromIdentifiers(stream, topic peekedIdentifier, partitionID uint32) (uint64, error) {
	if stream.named || topic.named {
		return 0, nil
	}
	return PackNamespace(stream.numeric, topic.numeric, partitionID)
}

// namespaceFromSendMessages peeks
// [metadata_len u32][stream ident][topic ident][partitioning kind u8, len u8, value].
// Only explicit PartitionId partitioning is routable: the broker never picks a
// partition under consensus.
func namespaceFromSendMessages(payload []byte) (uint64, error) {
	if len(payload) < 4 {
		return 0, ierror.ErrInvalidCommand
	}
	metadataLength := uint64(binary.LittleEndian.Uint32(payload))
	if uint64(len(payload)) < 4+metadataLength {
		return 0, ierror.ErrInvalidCommand
	}
	// A read past the declared metadata region must fail rather than spill
	// into message bytes and derive a namespace the server would not compute.
	metadata := payload[4 : 4+metadataLength]

	offset := 0
	stream, err := peekIdentifier(metadata, offset)
	if err != nil {
		return 0, err
	}
	offset += stream.length
	topic, err := peekIdentifier(metadata, offset)
	if err != nil {
		return 0, err
	}
	offset += topic.length

	if len(metadata) < offset+2 {
		return 0, ierror.ErrInvalidCommand
	}
	partitioningKind := metadata[offset]
	partitioningLength := int(metadata[offset+1])
	if partitioningKind != partitioningPartitionID {
		return 0, ierror.ErrFeatureUnavailable
	}
	if partitioningLength != 4 || len(metadata) < offset+2+4 {
		return 0, ierror.ErrInvalidCommand
	}
	partitionID := binary.LittleEndian.Uint32(metadata[offset+2:])

	return namespaceFromIdentifiers(stream, topic, partitionID)
}

// namespaceFromConsumerOffset peeks
// [consumer kind u8][consumer ident][stream ident][topic ident]
// [partition flag u8][partition u32]. The v1 and v2 request layouts share this
// prefix and differ only in the trailing fields, which routing ignores.
func namespaceFromConsumerOffset(payload []byte) (uint64, error) {
	if len(payload) < 1 || (payload[0] != 1 && payload[0] != 2) {
		return 0, ierror.ErrInvalidCommand
	}
	offset := 1
	consumer, err := peekIdentifier(payload, offset)
	if err != nil {
		return 0, err
	}
	offset += consumer.length
	stream, err := peekIdentifier(payload, offset)
	if err != nil {
		return 0, err
	}
	offset += stream.length
	topic, err := peekIdentifier(payload, offset)
	if err != nil {
		return 0, err
	}
	offset += topic.length

	if len(payload) < offset+5 {
		return 0, ierror.ErrInvalidCommand
	}
	if payload[offset] != 1 {
		return 0, ierror.ErrInvalidIdentifier
	}
	partitionID := binary.LittleEndian.Uint32(payload[offset+1:])

	return namespaceFromIdentifiers(stream, topic, partitionID)
}

// namespaceFromDeleteSegments peeks
// [stream ident][topic ident][partition u32].
func namespaceFromDeleteSegments(payload []byte) (uint64, error) {
	offset := 0
	stream, err := peekIdentifier(payload, offset)
	if err != nil {
		return 0, err
	}
	offset += stream.length
	topic, err := peekIdentifier(payload, offset)
	if err != nil {
		return 0, err
	}
	offset += topic.length

	if len(payload) < offset+4 {
		return 0, ierror.ErrInvalidCommand
	}
	partitionID := binary.LittleEndian.Uint32(payload[offset:])

	return namespaceFromIdentifiers(stream, topic, partitionID)
}
