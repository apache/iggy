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

/// The log records in every resource entry of `request`.
pub(crate) fn log_record_count(request: &ExportLogsServiceRequest) -> u64 {
    request
        .resource_logs
        .iter()
        .flat_map(|resource| &resource.scope_logs)
        .map(|scope| scope.log_records.len() as u64)
        .sum()
}

/// Log records the receiver accepted the request but dropped, with its reason.
pub(crate) fn rejected_log_records(response: &ExportLogsServiceResponse) -> Option<(i64, &str)> {
    response
        .partial_success
        .as_ref()
        .filter(|partial| partial.rejected_log_records > 0)
        .map(|partial| (partial.rejected_log_records, partial.error_message.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    fn resource(records_per_scope: &[usize]) -> ResourceLogs {
        ResourceLogs {
            scope_logs: records_per_scope
                .iter()
                .map(|records| ScopeLogs {
                    log_records: vec![LogRecord::default(); *records],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn given_request_should_count_every_record_across_resources_and_scopes() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![resource(&[2, 1]), resource(&[3])],
        };

        assert_eq!(log_record_count(&request), 6);
    }

    #[test]
    fn given_partial_success_without_rejections_should_not_report_rejected() {
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 0,
                error_message: "warning only".to_owned(),
            }),
        };

        assert!(rejected_log_records(&response).is_none());
    }

    #[test]
    fn given_partial_success_with_rejections_should_report_count_and_reason() {
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 2,
                error_message: "too old".to_owned(),
            }),
        };

        assert_eq!(rejected_log_records(&response), Some((2, "too old")));
    }
}
