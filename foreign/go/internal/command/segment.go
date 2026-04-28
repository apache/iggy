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

package command

import (
	"encoding/binary"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

type DeleteSegments struct {
	StreamId      iggcon.Identifier `json:"streamId"`
	TopicId       iggcon.Identifier `json:"topicId"`
	PartitionId   uint32            `json:"partitionId"`
	SegmentsCount uint32            `json:"segmentsCount"`
}

func (d *DeleteSegments) Code() Code {
	return DeleteSegmentsCode
}

func (d *DeleteSegments) MarshalBinary() ([]byte, error) {
	bytes, err := iggcon.MarshalIdentifiers(d.StreamId, d.TopicId)
	if err != nil {
		return nil, err
	}

	bytes = binary.LittleEndian.AppendUint32(bytes, d.PartitionId)
	bytes = binary.LittleEndian.AppendUint32(bytes, d.SegmentsCount)
	return bytes, nil
}
