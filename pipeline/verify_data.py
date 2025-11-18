#!/usr/bin/env python3
"""Quick script to verify data in ScyllaDB tables."""

import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

# Connect to ScyllaDB
scylla_hosts = os.getenv('SCYLLA_HOSTS').split(',')
scylla_port = int(os.getenv('SCYLLA_PORT', '19042'))
scylla_username = os.getenv('SCYLLA_USERNAME')
scylla_password = os.getenv('SCYLLA_PASSWORD')
scylla_keyspace = os.getenv('SCYLLA_KEYSPACE', 'iot_monitoring')

auth_provider = PlainTextAuthProvider(
    username=scylla_username,
    password=scylla_password
)

cluster = Cluster(
    scylla_hosts,
    port=scylla_port,
    auth_provider=auth_provider
)

session = cluster.connect(scylla_keyspace)

print("=" * 60)
print("ScyllaDB Data Verification")
print("=" * 60)

# Check raw metrics
print("\n📊 Raw Metrics (device_metrics_raw):")
result = session.execute("""
    SELECT device_id, date, timestamp, metric_name, metric_value, device_type
    FROM device_metrics_raw
    LIMIT 5
""")
for row in result:
    print(f"  {row.device_id} | {row.timestamp} | {row.metric_name} = {row.metric_value:.2f}")

# Count raw metrics
count = session.execute("SELECT COUNT(*) FROM device_metrics_raw").one()[0]
print(f"\n  Total raw metrics: {count}")

# Check snapshots
print("\n📦 Device State Snapshots (device_state_snapshots):")
result = session.execute("""
    SELECT device_id, snapshot_time, device_type, embedding_method
    FROM device_state_snapshots
    LIMIT 5
""")
for row in result:
    print(f"  {row.device_id} | {row.snapshot_time} | Method: {row.embedding_method}")

# Count snapshots
count = session.execute("SELECT COUNT(*) FROM device_state_snapshots").one()[0]
print(f"\n  Total snapshots: {count}")

# Show one full snapshot
print("\n🔍 Sample Snapshot Details:")
result = session.execute("""
    SELECT device_id, snapshot_time, metrics, embedding_method
    FROM device_state_snapshots
    LIMIT 1
""")
row = result.one()
if row:
    print(f"  Device: {row.device_id}")
    print(f"  Time: {row.snapshot_time}")
    print(f"  Embedding: {row.embedding_method}")
    print(f"  Metrics ({len(row.metrics)} total):")
    for metric_name, value in sorted(row.metrics.items())[:5]:
        print(f"    {metric_name}: {value:.2f}")
    if len(row.metrics) > 5:
        print(f"    ... and {len(row.metrics) - 5} more")

print("\n✅ Verification complete!")
cluster.shutdown()
