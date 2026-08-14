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

import "fmt"

type CompressionAlgorithm uint8

const (
	// CompressionAlgorithmDefault is the zero value of the field: the server
	// resolves the algorithm from its own configuration.
	CompressionAlgorithmDefault CompressionAlgorithm = 0
	CompressionAlgorithmNone    CompressionAlgorithm = 1
	CompressionAlgorithmGzip    CompressionAlgorithm = 2
)

// OptionValue renders the algorithm for the server's `compression_algorithm`
// topic option, or "" when the option should be omitted so the server resolves
// its own default.
//
// An unrecognized code is an error rather than a fall back to the default: the
// topic would otherwise be created with the server's algorithm and no
// diagnostic, where the old fixed-field wire format put the raw byte on the
// wire and let the server reject it.
func (c CompressionAlgorithm) OptionValue() (string, error) {
	switch c {
	case CompressionAlgorithmDefault, CompressionAlgorithmNone:
		return "", nil
	case CompressionAlgorithmGzip:
		return "gzip", nil
	default:
		return "", fmt.Errorf("unknown compression algorithm code %d", uint8(c))
	}
}

func (c CompressionAlgorithm) String() string {
	switch c {
	case CompressionAlgorithmDefault, CompressionAlgorithmNone:
		return "none"
	case CompressionAlgorithmGzip:
		return "gzip"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(c))
	}
}
