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

use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use crate::Client;
use anyhow::Context;
use async_trait::async_trait;
use iggy_common::ping::Ping;
use std::fmt::{Display, Formatter, Result};
use std::time::Duration;
use tokio::time::{sleep, Instant};
use tracing::{event, Level};

pub struct PingCmd {
    _ping: Ping,
    count: u32,
}

impl PingCmd {
    pub fn new(count: u32) -> Self {
        Self {
            _ping: Ping {},
            count,
        }
    }
}

struct PingStats {
    samples: Vec<u128>,
}

impl PingStats {
    fn new() -> Self {
        Self { samples: vec![] }
    }

    fn add(&mut self, ping_duration: &Duration) {
        self.samples.push(ping_duration.as_nanos());
    }

    fn count(&self) -> usize {
        self.samples.len()
    }

    fn get_min_avg_max(&self) -> (u128, u128, u128) {
        let (min, max, sum) = self
            .samples
            .iter()
            .fold((u128::MAX, u128::MIN, 0), |(min, max, sum), value| {
                (min.min(*value), max.max(*value), sum + value)
            });
        let avg = sum / self.count() as u128;

        (min, avg, max)
    }

    fn get_stats(&self) -> (u128, u128, u128, u128) {
        let (min, avg, max) = self.get_min_avg_max();

        let variance = self
            .samples
            .iter()
            .map(|value| {
                let diff = avg as f64 - (*value as f64);

                diff * diff
            })
            .sum::<f64>()
            / self.count() as f64;
        let std_dev = variance.sqrt() as u128;

        (min, avg, max, std_dev)
    }
}

fn nano_to_ms(nanoseconds: u128) -> f64 {
    nanoseconds as f64 / 1_000_000.0
}

impl Display for PingStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let (min, avg, max, std_dev) = self.get_stats();
        write!(
            f,
            "min/avg/max/mdev = {:.3}/{:.3}/{:.3}/{:.3} ms",
            nano_to_ms(min),
            nano_to_ms(avg),
            nano_to_ms(max),
            nano_to_ms(std_dev)
        )
    }
}

#[async_trait]
impl CliCommand for PingCmd {
    fn explain(&self) -> String {
        "ping command".to_owned()
    }

    fn login_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let print_width = (self.count.ilog10() + 1) as usize;
        let mut ping_stats = PingStats::new();

        for i in 1..=self.count {
            let time_start = Instant::now();
            client
                .ping()
                .await
                .with_context(|| "Problem sending ping command".to_owned())?;
            let ping_duration = time_start.elapsed();
            ping_stats.add(&ping_duration);
            event!(target: PRINT_TARGET, Level::INFO, "Ping sequence id: {:width$} time: {:.2} ms", i, nano_to_ms(ping_duration.as_nanos()), width = print_width);
            sleep(Duration::from_secs(1)).await;
        }

        event!(target: PRINT_TARGET, Level::INFO, "");
        event!(target: PRINT_TARGET, Level::INFO, "Ping statistics for {} ping commands", ping_stats.count());
        event!(target: PRINT_TARGET, Level::INFO, "{ping_stats}");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_add_samples() {
        let mut ping_stats = PingStats::new();

        ping_stats.add(&Duration::from_millis(1));
        ping_stats.add(&Duration::from_millis(2));
        ping_stats.add(&Duration::from_millis(3));
        ping_stats.add(&Duration::from_millis(4));
        ping_stats.add(&Duration::from_millis(5));
        ping_stats.add(&Duration::from_millis(6));

        assert_eq!(ping_stats.count(), 6);
    }

    #[test]
    fn should_get_min_avg_max() {
        let mut ping_stats = PingStats::new();

        ping_stats.add(&Duration::from_millis(1));
        ping_stats.add(&Duration::from_millis(9));

        assert_eq!(ping_stats.count(), 2);
        assert_eq!(ping_stats.get_min_avg_max(), (1000000, 5000000, 9000000));
    }

    #[test]
    fn should_return_stats() {
        let mut ping_stats = PingStats::new();

        ping_stats.add(&Duration::from_nanos(1));
        ping_stats.add(&Duration::from_nanos(3));
        ping_stats.add(&Duration::from_nanos(3));
        ping_stats.add(&Duration::from_nanos(3));
        ping_stats.add(&Duration::from_nanos(5));

        assert_eq!(ping_stats.count(), 5);
        assert_eq!(ping_stats.get_stats(), (1, 3, 5, 1));
    }

    #[test]
    fn should_format_stats() {
        let mut ping_stats = PingStats::new();

        ping_stats.add(&Duration::from_nanos(1322444));
        ping_stats.add(&Duration::from_nanos(3457432));
        ping_stats.add(&Duration::from_nanos(5343270));
        ping_stats.add(&Duration::from_nanos(7837541));

        assert_eq!(
            format!("{ping_stats}"),
            "min/avg/max/mdev = 1.322/4.490/7.838/2.400 ms"
        );
    }
}
