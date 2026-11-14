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

use std::io;
use std::thread;
use tokio::sync::{mpsc, oneshot};

/// Jobs that wait for the thread. A caller past this limit waits for a free slot, and a caller that
/// leaves while waiting takes its job with it. A queued job runs to the end even if its caller
/// leaves.
const QUEUE_LEN: usize = 16;

/// The thread is gone. While a `DedicatedThread` exists, only a panic in a job stops it.
#[derive(Debug)]
pub struct ThreadStopped;

/// Runs every job on the same OS thread, one at a time, in queue order.
pub struct DedicatedThread<J, R> {
    jobs: mpsc::Sender<(J, oneshot::Sender<R>)>,
}

impl<J: Send + 'static, R: Send + 'static> DedicatedThread<J, R> {
    pub fn start(name: &str, mut handle: impl FnMut(J) -> R + Send + 'static) -> io::Result<Self> {
        let (jobs, mut queue) = mpsc::channel::<(J, oneshot::Sender<R>)>(QUEUE_LEN);
        thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                while let Some((job, reply)) = queue.blocking_recv() {
                    // A caller that left dropped the receiver.
                    let _ = reply.send(handle(job));
                }
            })?;
        Ok(Self { jobs })
    }

    pub async fn run(&self, job: J) -> Result<R, ThreadStopped> {
        let (reply, result) = oneshot::channel();
        self.jobs
            .send((job, reply))
            .await
            .map_err(|_| ThreadStopped)?;
        result.await.map_err(|_| ThreadStopped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const TEST_THREAD: &str = "dedicated-test";
    /// More callers than queue slots, so some of them wait for a free slot.
    const CALLERS: usize = 2 * QUEUE_LEN;

    #[tokio::test]
    async fn given_more_callers_than_queue_slots_when_running_should_answer_each_on_one_thread() {
        let runner = Arc::new(
            DedicatedThread::start(TEST_THREAD, |job: usize| {
                (job, thread::current().name().map(str::to_owned))
            })
            .expect("the thread starts"),
        );

        let callers: Vec<_> = (0..CALLERS)
            .map(|job| {
                let runner = Arc::clone(&runner);
                tokio::spawn(async move { runner.run(job).await })
            })
            .collect();

        for (job, caller) in callers.into_iter().enumerate() {
            let (answered, ran_on) = caller
                .await
                .expect("the caller finishes")
                .expect("the thread answers");
            assert_eq!(answered, job);
            assert_eq!(ran_on.as_deref(), Some(TEST_THREAD), "job {job}");
        }
    }

    #[tokio::test]
    async fn given_job_that_panics_when_running_should_fail_it_and_every_later_job() {
        let runner = DedicatedThread::start(TEST_THREAD, |panics: bool| {
            assert!(!panics, "this job panics on purpose");
        })
        .expect("the thread starts");

        assert!(matches!(runner.run(true).await, Err(ThreadStopped)));
        assert!(matches!(runner.run(false).await, Err(ThreadStopped)));
    }
}
