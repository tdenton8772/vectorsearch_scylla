#!/usr/bin/env python3
"""Clear all data from IoT monitoring tables."""

import os
import argparse
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

# ScyllaDB connection
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

parser = argparse.ArgumentParser(description='Clear all IoT monitoring data')
parser.add_argument('--yes', action='store_true', help='Do not prompt for confirmation')
args = parser.parse_args()

print("🧹 Clearing ALL IoT monitoring data...")
print("⚠️  This will delete:")
print("  - All device snapshots")
print("  - All raw metrics")
print("  - All device profiles")
print("  - All anomaly events")
print("  - All statistics")
print("")

if not args.yes:
    confirm = input("Type 'yes' to confirm: ")
    if confirm.lower() != 'yes':
        print("Cancelled")
        exit(0)

print("\n🗑️  Truncating tables...")

# Clear all main tables
tables = [
    'device_metrics_raw',
    'device_state_snapshots',
    'device_profiles',
    'anomaly_events',
    'device_statistics',
    'metric_aggregation_buffer'
]

for table in tables:
    try:
        print(f"  - Truncating {table}...")
        session.execute(f"TRUNCATE {table}")
    except Exception as e:
        print(f"    Warning: {e}")

print("\n✅ All data cleared!")
print("\n📝 Next steps:")
print("  1. Start the pipeline: bash pipeline/start_pipeline.sh")
print("  2. Wait 5-10 minutes for data collection")
print("  3. Build profiles: python pipeline/build_profiles.py --all --days-back 1")

cluster.shutdown()
