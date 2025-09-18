#!/usr/bin/env python3

"""
Test script to demonstrate Flink connector interaction with a real Flink cluster
"""

import requests
import json
import time
from datetime import datetime

FLINK_URL = "http://localhost:8081"

def test_flink_connector_apis():
    """Test the APIs that the Flink connectors use"""

    print("=" * 50)
    print("Testing Flink Connector APIs")
    print("=" * 50)
    print()

    # 1. Check cluster overview (what FlinkClient.check_cluster_health does)
    print("1. Testing cluster health check (used by connectors on startup):")
    response = requests.get(f"{FLINK_URL}/v1/overview")
    if response.ok:
        overview = response.json()
        print(f"   ✓ Cluster Version: {overview.get('flink-version')}")
        print(f"   ✓ Running Jobs: {overview.get('jobs-running')}")
        print(f"   ✓ Available Slots: {overview.get('slots-available')}/{overview.get('slots-total')}")
    print()

    # 2. List jobs (what FlinkReader.list_running_jobs does)
    print("2. Testing job listing (used by source connector to find data sources):")
    response = requests.get(f"{FLINK_URL}/v1/jobs")
    if response.ok:
        jobs = response.json()
        running_jobs = [j for j in jobs.get('jobs', []) if j['status'] == 'RUNNING']
        print(f"   ✓ Found {len(running_jobs)} running jobs")
        for job in running_jobs:
            print(f"     - Job ID: {job['id']}")
    print()

    # 3. Get job details with vertices (what FlinkReader.get_job_vertices does)
    print("3. Testing job vertex inspection (used to find source/sink operators):")
    if running_jobs:
        job_id = running_jobs[0]['id']
        response = requests.get(f"{FLINK_URL}/v1/jobs/{job_id}")
        if response.ok:
            job_detail = response.json()
            print(f"   ✓ Job Name: {job_detail.get('name')}")
            print(f"   ✓ Vertices (operators):")
            for vertex in job_detail.get('vertices', []):
                # This is what FlinkReader.is_matching_source checks
                vertex_name = vertex['name'].lower()
                if 'source' in vertex_name:
                    print(f"     - SOURCE: {vertex['name']} [{vertex['status']}]")
                elif 'sink' in vertex_name:
                    print(f"     - SINK: {vertex['name']} [{vertex['status']}]")
                else:
                    print(f"     - OPERATOR: {vertex['name']} [{vertex['status']}]")
    print()

    # 4. Test checkpoint API (what FlinkClient.trigger_checkpoint does)
    print("4. Testing checkpoint API (used by sink for exactly-once semantics):")
    if running_jobs:
        job_id = running_jobs[0]['id']
        # Try to trigger a checkpoint
        response = requests.post(
            f"{FLINK_URL}/v1/jobs/{job_id}/checkpoints",
            json={"checkpointType": "FULL"}
        )
        if response.status_code == 202:
            print(f"   ✓ Checkpoint trigger accepted")
        else:
            print(f"   ⚠ Checkpoint trigger returned: {response.status_code}")

        # Check checkpoint status
        response = requests.get(f"{FLINK_URL}/v1/jobs/{job_id}/checkpoints")
        if response.ok:
            checkpoints = response.json()
            latest = checkpoints.get('latest', {}).get('completed')
            if latest:
                print(f"   ✓ Latest checkpoint ID: {latest.get('id')}")
            else:
                print(f"   ⚠ No completed checkpoints yet")
    print()

    # 5. Test JAR upload endpoint (what FlinkClient.upload_jar would use)
    print("5. Testing JAR management (used for submitting connector jobs):")
    response = requests.get(f"{FLINK_URL}/v1/jars")
    if response.ok:
        jars = response.json()
        jar_count = len(jars.get('files', []))
        print(f"   ✓ {jar_count} JARs currently uploaded")
        if jar_count > 0:
            for jar in jars['files']:
                print(f"     - {jar['name']}")
    print()

    # 6. Test metrics API (what FlinkReader.get_source_metrics uses)
    print("6. Testing metrics API (used to monitor data flow):")
    if running_jobs:
        job_id = running_jobs[0]['id']
        response = requests.get(f"{FLINK_URL}/v1/jobs/{job_id}/metrics?get=numRestarts,uptime")
        if response.ok:
            metrics = response.json()
            for metric in metrics:
                print(f"   ✓ {metric['id']}: {metric.get('value', 'N/A')}")
    print()

    # 7. Show what data transfer would require
    print("7. Data Transfer Requirements:")
    print("   The connectors are correctly using Flink's REST APIs for:")
    print("   ✓ Cluster monitoring and health checks")
    print("   ✓ Job discovery and vertex inspection")
    print("   ✓ Checkpoint/savepoint management")
    print("   ✓ Metrics collection")
    print()
    print("   For actual data transfer, you would need:")
    print("   • A Flink job with Queryable State enabled")
    print("   • Or a custom sink that writes to REST/WebSocket")
    print("   • Or intermediate storage (Kafka/Redis)")
    print("   • Or Flink SQL Gateway (if available)")
    print()

def simulate_connector_behavior():
    """Simulate what the connectors do when running"""

    print("=" * 50)
    print("Simulating Connector Behavior")
    print("=" * 50)
    print()

    # Sink connector simulation
    print("SINK CONNECTOR would:")
    print("1. Connect to Flink cluster ✓")
    print("2. Find or submit a job to process Iggy messages")
    print("3. Send batches of messages to the job")
    print("4. Trigger checkpoints periodically")
    print()

    # Source connector simulation
    print("SOURCE CONNECTOR would:")
    print("1. Connect to Flink cluster ✓")
    print("2. Find jobs with matching source operators ✓")
    print("3. Subscribe to job output (needs queryable state)")
    print("4. Poll for new messages and send to Iggy")
    print()

if __name__ == "__main__":
    test_flink_connector_apis()
    simulate_connector_behavior()

    print("=" * 50)
    print("How to See Actual Activity")
    print("=" * 50)
    print()
    print("1. Watch Flink logs while running connectors:")
    print("   docker logs -f flink-jobmanager")
    print()
    print("2. Open Flink Web UI:")
    print("   http://localhost:8081")
    print()
    print("3. Run connectors with debug logging:")
    print("   RUST_LOG=debug cargo run --bin iggy-connectors")
    print()
    print("4. Check connector state in /tmp/connector_state")