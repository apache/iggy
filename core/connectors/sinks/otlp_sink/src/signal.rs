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

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;

/// The export request of one OTLP signal.
///
/// Every request is one repeated field of resource entries, such as `ResourceLogs`. Protobuf
/// merges concatenated messages, so the encodings of several requests, joined, are one
/// request that holds all of their entries.
pub(crate) trait ExportRequest: prost::Message + Default {
    type Entry;
    type Response: ExportResponse;

    /// What the receiver counts in `partial_success` for this signal.
    const ITEM_NAME: &'static str;

    fn entries(&self) -> &[Self::Entry];
    /// The OTLP items in one entry, such as log records.
    fn item_count(entry: &Self::Entry) -> u64;
}

pub(crate) trait ExportResponse: prost::Message + Default + 'static {
    /// Items the receiver accepted the request but dropped, with its reason.
    fn rejected(&self) -> Option<(i64, &str)>;
}

impl ExportRequest for ExportLogsServiceRequest {
    type Entry = ResourceLogs;
    type Response = ExportLogsServiceResponse;

    const ITEM_NAME: &'static str = "log records";

    fn entries(&self) -> &[ResourceLogs] {
        &self.resource_logs
    }

    fn item_count(entry: &ResourceLogs) -> u64 {
        entry
            .scope_logs
            .iter()
            .map(|scope| scope.log_records.len() as u64)
            .sum()
    }
}

impl ExportResponse for ExportLogsServiceResponse {
    fn rejected(&self) -> Option<(i64, &str)> {
        self.partial_success
            .as_ref()
            .filter(|partial| partial.rejected_log_records > 0)
            .map(|partial| (partial.rejected_log_records, partial.error_message.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ScopeLogs};

    #[test]
    fn given_resource_logs_should_count_every_record_across_scopes() {
        let entry = ResourceLogs {
            scope_logs: vec![
                ScopeLogs {
                    log_records: vec![LogRecord::default(), LogRecord::default()],
                    ..Default::default()
                },
                ScopeLogs {
                    log_records: vec![LogRecord::default()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(ExportLogsServiceRequest::item_count(&entry), 3);
    }

    #[test]
    fn given_partial_success_without_rejections_should_not_report_rejected() {
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 0,
                error_message: "warning only".to_owned(),
            }),
        };

        assert!(response.rejected().is_none());
    }

    #[test]
    fn given_partial_success_with_rejections_should_report_count_and_reason() {
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 2,
                error_message: "too old".to_owned(),
            }),
        };

        assert_eq!(response.rejected(), Some((2, "too old")));
    }
}
