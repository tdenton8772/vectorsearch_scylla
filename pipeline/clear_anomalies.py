#!/usr/bin/env python3
"""Clear all anomaly data from the database."""

import os
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

print("🧹 Clearing anomaly data...")

# Clear anomaly_events table
print("  - Truncating anomaly_events table...")
session.execute("TRUNCATE anomaly_events")

# Reset anomalous flags in device_state_snapshots
print("  - Resetting anomalous flags in snapshots...")
result = session.execute("""
    SELECT device_id, date, snapshot_time 
    FROM device_state_snapshots 
    WHERE is_anomalous = true ALLOW FILTERING
""")

count = 0
for row in result:
    session.execute("""
        UPDATE device_state_snapshots 
        SET is_anomalous = false, anomaly_score = 0.0
        WHERE device_id = %s AND date = %s AND snapshot_time = %s
    """, (row.device_id, row.date, row.snapshot_time))
    count += 1

print(f"    Reset {count} anomalous snapshots")

# Reset anomaly counters
print("  - Resetting anomaly counters...")
devices = session.execute("SELECT device_id FROM device_statistics")
for row in devices:
    # Get current count
    result = session.execute(
        "SELECT anomaly_count FROM device_statistics WHERE device_id = %s",
        (row.device_id,)
    ).one()
    
    if result and result.anomaly_count > 0:
        # Subtract the count to reset to 0
        session.execute(
            "UPDATE device_statistics SET anomaly_count = anomaly_count - %s WHERE device_id = %s",
            (result.anomaly_count, row.device_id)
        )
        print(f"    Reset {row.device_id}: {result.anomaly_count} → 0")

print("✅ All anomaly data cleared!")
cluster.shutdown()
