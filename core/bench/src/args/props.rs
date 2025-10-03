/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::{output::BenchmarkOutputCommand, transport::BenchmarkTransportCommand};
use iggy::prelude::{IggyByteSize, TransportProtocol};

pub trait BenchmarkKindProps {
    fn streams(&self) -> usize;
    fn partitions(&self) -> usize;
    fn number_of_consumer_groups(&self) -> usize;
    fn consumers(&self) -> usize;
    fn producers(&self) -> usize;
    fn transport_command(&self) -> &BenchmarkTransportCommand;
    fn max_topic_size(&self) -> Option<IggyByteSize>;
    fn validate(&self);
    fn inner(&self) -> &dyn BenchmarkKindProps
    where
        Self: std::marker::Sized,
    {
        self
    }
}

pub trait BenchmarkTransportProps {
    fn transport(&self) -> &TransportProtocol;
    fn server_address(&self) -> &str;
    // TODO(hubcio): make benchmark use `client_address` and `validate_certificate`
    #[allow(dead_code)]
    fn client_address(&self) -> &str;
    #[allow(dead_code)]
    fn validate_certificate(&self) -> bool;
    fn output_command(&self) -> Option<&BenchmarkOutputCommand>;
    fn nodelay(&self) -> bool;
    fn inner(&self) -> &dyn BenchmarkTransportProps
    where
        Self: std::marker::Sized,
    {
        self
    }
}
