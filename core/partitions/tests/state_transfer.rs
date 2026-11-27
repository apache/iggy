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

use consensus::{StateArtifact, artifact_kind};
use partitions::state_transfer::{SegmentLoadError, load_verified_segment_artifact};

#[compio::test]
async fn given_segment_read_error_when_loading_artifact_should_classify_the_os_error() {
    // The open succeeds on a directory, and the read then fails with EISDIR.
    let directory = tempfile::tempdir().expect("temporary directory");
    let entry = StateArtifact {
        kind: artifact_kind::SEGMENT_LOG,
        frontier: 0,
        len: 1,
        checksum: 0,
    };

    let error =
        load_verified_segment_artifact(directory.path().to_str().expect("UTF-8 path"), &entry)
            .await
            .expect_err("a directory holds no segment bytes");

    assert!(
        matches!(&error, SegmentLoadError::Stale(source) if source.raw_os_error().is_some()),
        "the read error must reach the classification with its OS code, got {error:?}"
    );
}
