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

package tcp

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
)

func TestIggyTcpClient_DeleteSegments(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	streamId, _ := iggcon.NewIdentifier("stream")
	topicId, _ := iggcon.NewIdentifier(uint32(1))

	expectedCommand := command.DeleteSegments{
		StreamId:      streamId,
		TopicId:       topicId,
		PartitionId:   2,
		SegmentsCount: 3,
	}
	expectedPayload, err := expectedCommand.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize expected command: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		defer serverConn.Close()

		header := make([]byte, 8)
		if _, err := io.ReadFull(serverConn, header); err != nil {
			done <- err
			return
		}

		messageLength := binary.LittleEndian.Uint32(header[:4])
		commandCode := binary.LittleEndian.Uint32(header[4:8])
		payload := make([]byte, messageLength-4)
		if _, err := io.ReadFull(serverConn, payload); err != nil {
			done <- err
			return
		}

		if commandCode != uint32(command.DeleteSegmentsCode) {
			t.Errorf("Expected command code %d, got %d", command.DeleteSegmentsCode, commandCode)
		}
		if !bytes.Equal(payload, expectedPayload) {
			t.Errorf("Expected payload %v, got %v", expectedPayload, payload)
		}

		response := make([]byte, ResponseInitialBytesLength)
		_, err := serverConn.Write(response)
		done <- err
	}()

	client := &IggyTcpClient{
		conn: clientConn,
	}

	if err := client.DeleteSegments(streamId, topicId, 2, 3); err != nil {
		t.Fatalf("DeleteSegments failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Server side failed: %v", err)
	}
}
