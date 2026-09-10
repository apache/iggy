# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Validate analysis with synthetic observations; no server is started."""

import argparse
import csv
import json
import math
from pathlib import Path
import tempfile
import unittest

import poll_completion as runner


class PollAnalysisTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="iggy-poll-analysis-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def write_polls(self, rows):
        path = self.directory / "polls.csv"
        with path.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=[
                "actor_id", "elapsed_us", "poll_latency_us", "messages", "outcome",
            ])
            writer.writeheader()
            writer.writerows(rows)
        return path

    def comparison(self, ratios, candidate_latency=1.0):
        pairs = []
        for index, ratio in enumerate(ratios):
            pair = {"case": "disk-next", "pair": index}
            for version in ("baseline", "candidate"):
                name = f"{index}-{version}"
                pair[version] = name
                directory = self.directory / name
                directory.mkdir()
                runner.save(directory / "experiment.json", {
                    "status": "completed", "platform": "linux", "case": "disk-next",
                    "client_sha256": "same-client", "server_sha256": f"{version}-server", "polls": 100, "batch": 1,
                    "background_rate_bytes_per_second": 0,
                })
                baseline_rate = 100 if index % 2 == 0 else 1000
                runner.save(directory / "summary.json", {
                    "outcomes": {"messages": 100},
                    "polls_per_second": baseline_rate * (ratio if version == "candidate" else 1),
                    "p99_poll_us": 100 * (candidate_latency if version == "candidate" else 1),
                })
            pairs.append(pair)
        schedule = self.directory / "schedule.json"
        runner.save(schedule, {"pairs": pairs})
        return argparse.Namespace(
            schedule=str(schedule), results=str(self.directory),
            output=str(self.directory / "comparison.json"), seed=42, resamples=500,
        )

    def test_raw_quantiles_exclude_failed_polls_but_elapsed_work_does_not(self):
        path = self.write_polls([
            {"actor_id": 1, "elapsed_us": 250000, "poll_latency_us": 10, "messages": 1, "outcome": "messages"},
            {"actor_id": 1, "elapsed_us": 500000, "poll_latency_us": 100, "messages": 2, "outcome": "messages"},
            {"actor_id": 1, "elapsed_us": 750000, "poll_latency_us": 20, "messages": 3, "outcome": "messages"},
            {"actor_id": 1, "elapsed_us": 1000000, "poll_latency_us": 999999, "messages": 0, "outcome": "empty"},
            {"actor_id": 1, "elapsed_us": 2000000, "poll_latency_us": 999999, "messages": 0, "outcome": "error"},
        ])
        summary = runner.poll_summary(path)
        self.assertEqual(summary["completed_polls"], 3)
        self.assertEqual(summary["completed_messages"], 6)
        self.assertEqual(summary["measurement_seconds"], 2)
        self.assertEqual(summary["polls_per_second"], 1.5)
        self.assertEqual(summary["messages_per_second"], 3)
        self.assertEqual(summary["p50_poll_us"], 20)
        self.assertAlmostEqual(summary["p99_poll_us"], 98.4)
        self.assertEqual(summary["outcomes"], {"messages": 3, "empty": 1, "error": 1})

    def test_poll_summary_rejects_empty_and_unsynchronized_actor_data(self):
        empty = {"actor_id": 1, "elapsed_us": 10, "poll_latency_us": 10, "messages": 0, "outcome": "empty"}
        with self.assertRaisesRegex(RuntimeError, "No completed polls"):
            runner.poll_summary(self.write_polls([empty]))
        message = dict(empty, messages=1, outcome="messages")
        with self.assertRaisesRegex(RuntimeError, "one consumer"):
            runner.poll_summary(self.write_polls([message, dict(message, actor_id=2)]))

    def test_cpu_crop_uses_only_bracketed_measurement_window(self):
        samples = [
            {"unix_us": 10, "cpu_seconds": 100},
            {"unix_us": 20, "cpu_seconds": 102},
            {"unix_us": 40, "cpu_seconds": 108},
        ]
        before = runner.interpolate(samples, 15, "unix_us", "cpu_seconds")
        after = runner.interpolate(samples, 35, "unix_us", "cpu_seconds")
        self.assertEqual(after - before, 5.5)
        for timestamp in (9, 41):
            with self.assertRaisesRegex(RuntimeError, "not bracketed"):
                runner.interpolate(samples, timestamp, "unix_us", "cpu_seconds")

    def test_producer_crop_translates_consumer_wall_clock_to_actor_elapsed(self):
        producer_started = 1000000
        consumer_started, consumer_ended = 1050000, 1150000
        samples = [
            {"elapsed_us": 0, "payload_bytes": 0},
            {"elapsed_us": 100000, "payload_bytes": 1000},
            {"elapsed_us": 200000, "payload_bytes": 5000},
        ]
        before = runner.interpolate(samples, consumer_started - producer_started, "elapsed_us", "payload_bytes")
        after = runner.interpolate(samples, consumer_ended - producer_started, "elapsed_us", "payload_bytes")
        self.assertEqual((after - before) * 1e6 / (consumer_ended - consumer_started), 25000)

    def test_schedule_preserves_counterbalanced_order_and_seed(self):
        args = argparse.Namespace(cases=["resident", "disk-next"], pairs=10, seed=43, output=str(self.directory / "schedule.json"))
        runner.schedule(args)
        first = Path(args.output).read_text()
        runner.schedule(args)
        self.assertEqual(first, Path(args.output).read_text())
        pairs = json.loads(first)["pairs"]
        for case in args.cases:
            matching = [pair for pair in pairs if pair["case"] == case]
            self.assertEqual(len(matching), 10)
            self.assertEqual(sum(pair["order"][0] == "baseline" for pair in matching), 5)
            self.assertEqual(len({pair[version] for pair in matching for version in ("baseline", "candidate")}), 20)

    def test_bootstrap_uses_independent_pair_ratios_and_is_reproducible(self):
        args = self.comparison([2, 0.5] * 5)
        runner.compare(args)
        first = Path(args.output).read_text()
        runner.compare(args)
        self.assertEqual(first, Path(args.output).read_text())
        result = json.loads(first)["cases"]["disk-next"]
        self.assertAlmostEqual(result["bounds"]["throughput_ratio"]["geometric_mean"], 1)
        self.assertNotAlmostEqual(result["bounds"]["throughput_ratio"]["geometric_mean"], 700 / 1100)
        self.assertEqual(len(result["pairs"]), 10)
        self.assertTrue(math.isfinite(result["bounds"]["throughput_ratio"]["one_sided_95_bound"]))

    def test_constant_regression_and_insufficient_replication_are_distinct(self):
        args = self.comparison([0.9] * 10, candidate_latency=1.2)
        runner.compare(args)
        result = json.loads(Path(args.output).read_text())["cases"]["disk-next"]
        self.assertEqual(result["decision"], "confirmed regression")
        plan = json.loads(Path(args.schedule).read_text())
        plan["pairs"].pop()
        runner.save(args.schedule, plan)
        runner.compare(args)
        result = json.loads(Path(args.output).read_text())["cases"]["disk-next"]
        self.assertEqual(result["decision"], "inconclusive")

    def test_comparison_rejects_client_mismatch_and_incomplete_work(self):
        args = self.comparison([1] * 10)
        path = self.directory / "0-candidate" / "experiment.json"
        original = json.loads(path.read_text())
        runner.save(path, dict(original, client_sha256="different-client"))
        with self.assertRaisesRegex(RuntimeError, "different clients"):
            runner.compare(args)
        runner.save(path, original)
        path = self.directory / "0-candidate" / "summary.json"
        summary = json.loads(path.read_text())
        summary["outcomes"]["cancelled"] = 1
        runner.save(path, summary)
        with self.assertRaisesRegex(RuntimeError, "Unexpected poll outcomes"):
            runner.compare(args)


    def test_comparison_rejects_schedule_mismatch_and_binary_changes_between_pairs(self):
        args = self.comparison([1] * 10)
        path = self.directory / "0-candidate" / "experiment.json"
        original = json.loads(path.read_text())
        runner.save(path, dict(original, case="resident"))
        with self.assertRaisesRegex(RuntimeError, "scheduled case"):
            runner.compare(args)
        runner.save(path, original)
        for version in ("baseline", "candidate"):
            path = self.directory / f"1-{version}" / "experiment.json"
            experiment = json.loads(path.read_text())
            experiment["client_sha256"] = "another-client-for-this-pair"
            runner.save(path, experiment)
        with self.assertRaisesRegex(RuntimeError, "binary changed"):
            runner.compare(args)


if __name__ == "__main__":
    unittest.main()
