#!/usr/bin/env python3
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

"""Run isolated poll experiments on Linux or macOS and compare paired samples."""

import argparse
import collections
import csv
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time


CASES = {
    "resident": (1, "next", False, False, False),
    "disk-offset": (1, "offset", False, True, False),
    "disk-next": (1, "next", False, True, False),
    "disk-batch": (100, "next", False, True, False),
    "disk-group": (100, "next", True, True, False),
    "disk-writes": (100, "next", False, True, True),
    "disk-fsync": (100, "next", False, True, True),
}


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def cpu_prefix(cpus):
    return ["taskset", "-c", cpus] if sys.platform == "linux" else []


def stop(process):
    if process and process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def execute(command, cwd, log, deadline=60, env=None):
    with open(log, "w") as output:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=output, stderr=output)
        try:
            code = process.wait(timeout=deadline)
        except subprocess.TimeoutExpired:
            stop(process)
            raise RuntimeError(f"External deadline exceeded: {command}; see {log}") from None
        if code:
            raise RuntimeError(f"Command exited {code}: {command}; see {log}")


def process_sample(pid):
    if sys.platform == "darwin":
        # RUSAGE_INFO_V0 from the macOS SDK's sys/resource.h.
        class RusageInfo(ctypes.Structure):
            _fields_ = [("uuid", ctypes.c_uint8 * 16), ("values", ctypes.c_uint64 * 10)]

        usage = RusageInfo()
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        if library.proc_pid_rusage(pid, 0, ctypes.byref(usage)) != 0:
            raise OSError(ctypes.get_errno(), "proc_pid_rusage failed")
        class TimebaseInfo(ctypes.Structure):
            _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

        timebase = TimebaseInfo()
        system = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
        if system.mach_timebase_info(ctypes.byref(timebase)) != 0 or not timebase.denom:
            raise RuntimeError("Cannot convert native CPU clock ticks")
        return {
            "time": time.monotonic(), "unix_us": time.time_ns() // 1000,
            "cpu_seconds": (usage.values[0] + usage.values[1]) * timebase.numer / timebase.denom / 1e9,
            "rss_bytes": usage.values[6],
        }
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return {
        "time": time.monotonic(),
        "unix_us": time.time_ns() // 1000,
        "cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
        "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
    }


def percentile(values, quantile):
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def interpolate(samples, timestamp, clock_key, value_key):
    for left, right in zip(samples, samples[1:]):
        if left[clock_key] <= timestamp <= right[clock_key]:
            span = right[clock_key] - left[clock_key]
            fraction = (timestamp - left[clock_key]) / span if span else 0
            return left[value_key] + fraction * (right[value_key] - left[value_key])
    raise RuntimeError(f"Measurement window is not bracketed by {value_key} samples")


def poll_summary(path):
    with open(path, newline="") as source:
        rows = list(csv.DictReader(source))
    outcomes = collections.Counter(row["outcome"] for row in rows)
    completed = [row for row in rows if row["outcome"] == "messages"]
    if not completed:
        raise RuntimeError(f"No completed polls in {path}")
    actors = {row["actor_id"] for row in rows}
    if len(actors) != 1:
        raise RuntimeError("This runner requires one consumer; actor clocks are not synchronized")
    duration = max(int(row["elapsed_us"]) for row in rows) / 1e6
    latencies = [int(row["poll_latency_us"]) for row in completed]
    messages = sum(int(row["messages"]) for row in completed)
    return {
        "completed_polls": len(completed),
        "completed_messages": messages,
        "measurement_seconds": duration,
        "polls_per_second": len(completed) / duration,
        "messages_per_second": messages / duration,
        "p50_poll_us": percentile(latencies, 0.50),
        "p99_poll_us": percentile(latencies, 0.99),
        "outcomes": dict(outcomes),
        "percentile_population": "all successful nonempty polls, pooled before quantiles",
    }


class Experiment:
    def __init__(self, args):
        self.args = args
        maximum = 2_000_000 if args.case == "resident" else 8_000_000
        if not 0 < args.polls * CASES[args.case][0] <= maximum:
            raise ValueError(f"Fixture must contain 1 to {maximum:,} messages")
        self.directory = Path(args.output).resolve()
        self.directory.mkdir(parents=True, exist_ok=False)
        self.fixture = self.directory / "fixture"
        self.fixture.mkdir()
        self.batch, self.kind, self.group, self.disk, self.background = CASES[args.case]
        if args.calibrate_producer:
            self.background = False
        self.commands = []
        self.sequence = 0
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.address = f"127.0.0.1:{self.port}"

    def command(self, command, label, deadline=120):
        self.commands.append({"label": label, "argv": command})
        save(self.directory / "commands.json", self.commands)
        self.sequence += 1
        execute(command, self.fixture, self.directory / f"{self.sequence:02}-{label}.log", deadline)

    def cli(self, *arguments):
        self.command(cpu_prefix("1-2") + [self.args.cli, "--transport", "tcp", "--tcp-server-address", self.address, "--username", "iggy", "--password", "iggy", *arguments], arguments[0] + "-" + arguments[1])

    def bench(self, label, batches, batch, producer=False, rate=None):
        command = cpu_prefix("1-2") + [self.args.client, "--message-size", "256", "--messages-per-batch", str(batch), "--message-batches", str(batches), "--warmup-time", "0s", "--reuse-streams"]
        if rate:
            command += ["--rate-limit", str(rate)]
        if producer:
            command += ["pinned-producer", "--streams", "1", "--producers", "1"]
        else:
            command += ["--polling-kind", self.kind, "--latency-kind", "poll"]
            command += ["balanced-consumer-group" if self.group else "pinned-consumer", "--streams", "1", "--consumers", "1"]
            if self.group:
                command += ["--consumer-groups", "1"]
        return command + ["tcp", "--server-address", self.address, "--nodelay", "output", "--output-dir", str(self.directory / label), "--gitref", self.args.label, "--remark", self.args.case]

    def fixture_create(self, messages, label):
        self.cli("stream", "create", "bench-stream-1")
        options = {
            "segment_size": "16MiB" if self.disk else "1GiB",
            "messages_required_to_save": "1" if self.disk else "16777216",
            "size_of_messages_required_to_save": "1GiB",
            "preallocate_segments": "false",
            "enforce_fsync": "true" if self.args.case == "disk-fsync" else "false",
        }
        command = ["topic", "create", "bench-stream-1", "topic-1", "1", "none", "unlimited", "--max-topic-size", "unlimited"]
        for key, value in options.items():
            command += ["--set", f"{key}={value}"]
        self.cli(*command)
        self.command(self.bench(label + "-produce", math.ceil(messages / 1000), 1000, producer=True), label + "-produce", deadline=180)
        self.cli("topic", "get", "bench-stream-1", "topic-1")

    def start_server(self, env):
        with open(self.directory / "server.log", "a") as output:
            server = subprocess.Popen(cpu_prefix("0") + [self.args.server, "--with-default-root-credentials"], cwd=self.fixture, env=env, stdout=output, stderr=output)
        try:
            for _ in range(300):
                if server.poll() is not None:
                    raise RuntimeError("Server exited before readiness; inspect server.log")
                try:
                    with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                        return server
                except OSError:
                    time.sleep(0.1)
            raise RuntimeError("Server readiness deadline exceeded")
        except BaseException:
            stop(server)
            raise

    def run(self):
        server = background = None
        samples = []
        done = threading.Event()
        env = {key: value for key, value in os.environ.items() if not key.startswith("IGGY_")}
        env.update({"IGGY_SYSTEM_SHARDING_CPU_ALLOCATION": "1", "IGGY_SYSTEM_SHARDING_PIN_CORES": "true" if sys.platform == "linux" else "false"})
        env.update({"IGGY_TCP_ADDRESS": self.address, "IGGY_HTTP_ENABLED": "false", "IGGY_QUIC_ENABLED": "false", "IGGY_WEBSOCKET_ENABLED": "false"})
        env["IGGY_SYSTEM_PATH"] = str(self.fixture / "local_data")
        metadata = {
            "case": self.args.case, "label": self.args.label,
            "polls": self.args.polls, "batch": self.batch,
            "background_rate_bytes_per_second": self.args.background_rate,
            "server_sha256": digest(self.args.server), "client_sha256": digest(self.args.client),
            "cli_sha256": digest(self.args.cli),
            "server_environment": {key: value for key, value in env.items() if key.startswith("IGGY_")},
            "platform": sys.platform,
            "server_cpu": "0" if sys.platform == "linux" else "OS scheduled, one shard",
            "client_cpus": "1-2" if sys.platform == "linux" else "OS scheduled, two Tokio worker threads",
            "deadline_seconds": self.args.deadline,
            "status": "running", "started_at_unix": time.time(),
        }
        save(self.directory / "experiment.json", metadata)
        try:
            server = self.start_server(env)
            self.fixture_create(max(1000, 100 * self.batch), "warm")
            self.command(self.bench("warm-consume", 100, self.batch), "warm-consume", self.args.deadline)
            self.cli("stream", "delete", "bench-stream-1")
            self.fixture_create(1000 if self.args.calibrate_producer else self.args.polls * self.batch, "fixture")
            if self.args.calibrate_producer:
                self.command(self.bench("producer-calibration", self.args.polls, self.batch, producer=True), "producer-calibration", self.args.deadline)
                manifests = list((self.directory / "producer-calibration").rglob("run-manifest.json"))
                if len(manifests) != 1:
                    raise RuntimeError("Missing producer calibration manifest")
                manifest = json.loads(manifests[0].read_text())
                producer = manifest["producers"][0]
                if manifest["status"] != "completed":
                    raise RuntimeError("Producer calibration did not complete")
                save(self.directory / "summary.json", {
                    "payload_bytes_per_second": producer["payload_bytes"] * 1e6 / producer["measurement_duration_us"],
                    "measurement_seconds": producer["measurement_duration_us"] / 1e6,
                    "completed_batches": producer["completed_batches"],
                    "completed_messages": producer["completed_messages"],
                })
                metadata["status"] = "completed"
                return
            if self.background:
                if not self.args.background_rate:
                    raise RuntimeError("Background cases require a rate calibrated on the baseline")
                # Bound background data even when a consumer fails to finish.
                batches = math.ceil(self.args.background_rate * (self.args.deadline + 5) / (256 * 100))
                command = self.bench("background", batches, 100, producer=True, rate=self.args.background_rate)
                self.commands.append({"label": "background", "argv": command})
                with open(self.directory / "background.log", "w") as output:
                    background = subprocess.Popen(command, cwd=self.fixture, stdout=output, stderr=output)
                time.sleep(1)
                if background.poll() is not None:
                    raise RuntimeError("Background producer exited before measurement")

            def sample():
                while not done.is_set():
                    try:
                        samples.append(process_sample(server.pid))
                    except (OSError, ValueError):
                        break
                    done.wait(0.05)

            monitor = threading.Thread(target=sample, daemon=True)
            monitor.start()
            try:
                self.command(self.bench("measured", self.args.polls, self.batch), "measured", self.args.deadline)
            finally:
                done.set()
                monitor.join()
                samples.append(process_sample(server.pid))
            if background and background.poll() is not None:
                raise RuntimeError("Background producer exited during measurement")
            raw = list((self.directory / "measured").rglob("polls.csv"))
            if len(raw) != 1:
                raise RuntimeError(f"Expected one poll CSV, found {len(raw)}")
            summary = poll_summary(raw[0])
            manifest = json.loads(raw[0].with_name("run-manifest.json").read_text())
            if manifest["status"] != "completed" or len(manifest["actors"]) != 1:
                raise RuntimeError("Measured consumer did not complete with exactly one actor")
            started = manifest["actors"][0]["measurement_started_at_unix_us"]
            ended = started + round(summary["measurement_seconds"] * 1e6)
            expected = self.args.polls * self.batch
            if summary["completed_messages"] != expected:
                raise RuntimeError(f"Completed {summary['completed_messages']} messages, expected {expected}")
            summary.update({
                "server_cpu_seconds": interpolate(samples, ended, "unix_us", "cpu_seconds") - interpolate(samples, started, "unix_us", "cpu_seconds"),
                "peak_server_rss_bytes": max(point["rss_bytes"] for point in samples if started - 50000 <= point["unix_us"] <= ended + 50000),
                "cpu_window_seconds": (ended - started) / 1e6,
                "cpu_window": "consumer measurement; CPU counters interpolated from 50 ms samples",
            })
            stop(background)
            if background:
                manifests = list((self.directory / "background").rglob("run-manifest.json"))
                if len(manifests) != 1:
                    raise RuntimeError("Missing background producer manifest")
                producer = json.loads(manifests[0].read_text())["producers"][0]
                with open(manifests[0].with_name("producer-samples.csv"), newline="") as source:
                    points = [{key: int(value) for key, value in row.items()} for row in csv.DictReader(source)]
                origin = producer["measurement_started_at_unix_us"]
                produced = interpolate(points, ended - origin, "elapsed_us", "payload_bytes") - interpolate(points, started - origin, "elapsed_us", "payload_bytes")
                summary["background_payload_bytes_per_second"] = produced / summary["measurement_seconds"]
                summary["background_counter_resolution"] = "approximately 100 ms; interpolated over consumer window"
            summary["server_cpu_seconds_per_completed_poll"] = summary["server_cpu_seconds"] / summary["completed_polls"]
            save(self.directory / "summary.json", summary)
            save(self.directory / "server-samples.json", samples)
            metadata["status"] = "completed"
        except BaseException as error:
            metadata["status"] = "failed"
            metadata["failure"] = str(error)
            raise
        finally:
            stop(background)
            stop(server)
            save(self.directory / "server-samples.json", samples)
            save(self.directory / "experiment.json", metadata)
            if not self.args.keep_fixture:
                shutil.rmtree(self.fixture)


def compare(args):
    schedule = json.loads(Path(args.schedule).read_text())
    rows = collections.defaultdict(list)
    clients = set()
    platforms = set()
    servers = collections.defaultdict(set)
    for pair in schedule["pairs"]:
        runs = []
        for version in ("baseline", "candidate"):
            directory = Path(args.results) / pair[version]
            experiment = json.loads((directory / "experiment.json").read_text())
            summary = json.loads((directory / "summary.json").read_text())
            if experiment["status"] != "completed":
                raise RuntimeError(f"Incomplete run: {directory}")
            if experiment["case"] != pair["case"]:
                raise RuntimeError(f"Run does not match scheduled case: {directory}")
            clients.add(experiment["client_sha256"])
            platforms.add(experiment["platform"])
            servers[version].add(experiment["server_sha256"])
            if any(summary["outcomes"].get(kind, 0) for kind in ("empty", "error", "timeout", "cancelled")):
                raise RuntimeError(f"Unexpected poll outcomes: {directory}")
            runs.append((experiment, summary))
        if runs[0][0]["client_sha256"] != runs[1][0]["client_sha256"]:
            raise RuntimeError("Baseline and candidate used different clients")
        for key in ("platform", "case", "polls", "batch", "background_rate_bytes_per_second"):
            if runs[0][0][key] != runs[1][0][key]:
                raise RuntimeError(f"Paired workloads differ: {key}")
        rows[pair["case"]].append({
            "pair": pair["pair"],
            "throughput_ratio": runs[1][1]["polls_per_second"] / runs[0][1]["polls_per_second"],
            "p99_ratio": runs[1][1]["p99_poll_us"] / runs[0][1]["p99_poll_us"],
            "background_payload_ratio": (runs[1][1]["background_payload_bytes_per_second"] / runs[0][1]["background_payload_bytes_per_second"]) if CASES[pair["case"]][4] else None,
        })
    if len(clients) != 1 or len(platforms) != 1 or any(len(hashes) != 1 for hashes in servers.values()):
        raise RuntimeError("Platform or binary changed within the comparison")
    randomizer = random.Random(args.seed)
    result = {"method": "paired bootstrap of geometric mean ratios, one-sided 95% bounds; independent runs are the sampling units", "seed": args.seed, "resamples": args.resamples, "cases": {}}
    for case, pairs in rows.items():
        count = len(pairs)
        bounds = {}
        for metric, quantile in (("throughput_ratio", 0.05), ("p99_ratio", 0.95)):
            logs = [math.log(pair[metric]) for pair in pairs]
            bootstrap = [math.exp(statistics.mean(randomizer.choices(logs, k=count))) for _ in range(args.resamples)]
            bounds[metric] = {"geometric_mean": math.exp(statistics.mean(logs)), "one_sided_95_bound": percentile(bootstrap, quantile), "lower_95_bound": percentile(bootstrap, 0.05), "upper_95_bound": percentile(bootstrap, 0.95)}
        passes = count >= 10 and bounds["throughput_ratio"]["one_sided_95_bound"] >= 0.97 and bounds["p99_ratio"]["one_sided_95_bound"] <= 1.05
        regression = count >= 10 and (bounds["throughput_ratio"]["upper_95_bound"] < 0.97 or bounds["p99_ratio"]["lower_95_bound"] > 1.05)
        result["cases"][case] = {"pairs": pairs, "bounds": bounds, "decision": "preliminary compliance" if passes else "confirmed regression" if regression else "inconclusive"}
    save(args.output, result)


def schedule(args):
    randomizer = random.Random(args.seed)
    pairs = []
    for case in args.cases:
        orders = [["baseline", "candidate"], ["candidate", "baseline"]] * math.ceil(args.pairs / 2)
        randomizer.shuffle(orders)
        for index, order in enumerate(orders[:args.pairs]):
            names = {version: f"{case}/{index:02}-{version}" for version in order}
            pairs.append({"case": case, "pair": index, "order": order, **names})
    save(args.output, {"seed": args.seed, "pairs": pairs})


def matrix(args):
    workload = json.loads(Path(args.workload).read_text())
    plan = json.loads(Path(args.schedule).read_text())
    for pair in plan["pairs"]:
        for version in pair["order"]:
            directory = Path(args.results).resolve() / pair[version]
            settings = workload[pair["case"]]
            run = argparse.Namespace(
                server=getattr(args, version), client=args.client, cli=args.cli,
                output=str(directory), label=version, case=pair["case"],
                polls=settings["polls"], deadline=args.deadline,
                background_rate=settings.get("background_rate", 0),
                keep_fixture=False, calibrate_producer=False,
            )
            if directory.exists():
                raise RuntimeError(f"Run directory already exists: {directory}; preserve results and use a new schedule")
            Experiment(run).run()
            result = json.loads((directory / "summary.json").read_text())
            print(json.dumps({"case": pair["case"], "pair": pair["pair"], "version": version, "summary": result}), flush=True)


def main():
    os.environ["TOKIO_WORKER_THREADS"] = "2"
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    for flag in ("server", "client", "cli", "output", "label"):
        run.add_argument("--" + flag, required=True)
    run.add_argument("--case", choices=CASES, required=True)
    run.add_argument("--polls", type=int, required=True)
    run.add_argument("--deadline", type=int, default=40)
    run.add_argument("--background-rate", type=int, default=0)
    run.add_argument("--keep-fixture", action="store_true")
    run.add_argument("--calibrate-producer", action="store_true")
    plan = commands.add_parser("schedule")
    plan.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    plan.add_argument("--pairs", type=int, default=10)
    plan.add_argument("--seed", type=int, default=20260909)
    plan.add_argument("--output", required=True)
    analysis = commands.add_parser("compare")
    analysis.add_argument("--schedule", required=True)
    analysis.add_argument("--results", required=True)
    analysis.add_argument("--output", required=True)
    analysis.add_argument("--seed", type=int, default=20260909)
    analysis.add_argument("--resamples", type=int, default=10000)
    series = commands.add_parser("matrix")
    for flag in ("baseline", "candidate", "client", "cli", "schedule", "workload", "results"):
        series.add_argument("--" + flag, required=True)
    series.add_argument("--deadline", type=int, default=40)
    args = parser.parse_args()
    if args.command == "run":
        maximum = 2_000_000 if args.case == "resident" else 8_000_000
        if args.polls <= 0 or args.polls * CASES[args.case][0] > maximum:
            parser.error(f"Fixture must contain 1 to {maximum:,} messages")
        Experiment(args).run()
    elif args.command == "schedule":
        schedule(args)
    elif args.command == "matrix":
        matrix(args)
    else:
        compare(args)


if __name__ == "__main__":
    main()
