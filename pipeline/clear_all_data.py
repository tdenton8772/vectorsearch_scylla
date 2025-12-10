#!/usr/bin/env python3
"""Clear IoT monitoring data.

Two modes:
- Windowed (default): delete only old data (e.g., older than N minutes). Profiles are preserved.
- Hard reset (--hard-reset): truncate all main tables (optionally including profiles with --include-profiles).
"""

import os
import argparse
from datetime import datetime, timezone, timedelta
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

parser = argparse.ArgumentParser(description='Clear IoT monitoring data')
parser.add_argument('--yes', action='store_true', help='Do not prompt for confirmation')
parser.add_argument('--older-than-minutes', type=int, default=15, help='Delete data older than this many minutes (windowed mode)')
parser.add_argument('--hard-reset', action='store_true', help='TRUNCATE tables instead of windowed deletes')
parser.add_argument('--include-profiles', action='store_true', help='In hard reset mode, also truncate device_profiles')
args = parser.parse_args()

if args.hard_reset:
    print("🧹 Clearing ALL IoT monitoring data (HARD RESET)...")
    print("⚠️  This will delete:")
    print("  - All device snapshots")
    print("  - All raw metrics")
    print("  - All anomaly events")
    print("  - All statistics")
    if args.include_profiles:
        print("  - All device profiles (requested)")
    print("")
else:
    print("🧹 Windowed cleanup: deleting data older than", args.older_than_minutes, "minute(s)")
    print("⚠️  This will delete:")
    print("  - Old raw metrics (device_metrics_raw)")
    print("  - Old snapshots (device_state_snapshots)")
    print("  - Old aggregation buffers (metric_aggregation_buffer)")
    print("Profiles will NOT be deleted.")
    print("")

if not args.yes:
    confirm = input("Type 'yes' to confirm: ")
    if confirm.lower() != 'yes':
        print("Cancelled")
        cluster.shutdown()
        raise SystemExit(0)

if args.hard_reset:
    print("\n🗑️  Truncating tables...")
    tables = [
        'device_metrics_raw',
        'device_state_snapshots',
        'anomaly_events',
        'device_statistics',
        'metric_aggregation_buffer'
    ]
    if args.include_profiles:
        tables.insert(2, 'device_profiles')

    for table in tables:
        try:
            print(f"  - Truncating {table}...")
            session.execute(f"TRUNCATE {table}")
        except Exception as e:
            print(f"    Warning: {e}")

    print("\n✅ Hard reset complete!")
else:
    # Windowed deletes
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=args.older_than_minutes)
    print(f"\n🗑️  Deleting rows older than {args.older_than_minutes} minute(s) (cutoff: {cutoff.isoformat()})")

    # Helper: get devices from profiles (preserved)
    device_ids = [r.device_id for r in session.execute("SELECT device_id FROM device_profiles")]
    if not device_ids:
        # Fallback: try reading a few device_ids from recent snapshots today (best-effort)
        today_str = now.strftime('%Y-%m-%d')
        try:
            device_ids = list({r.device_id for r in session.execute(
                "SELECT device_id FROM device_state_snapshots WHERE date=%s LIMIT 100 ALLOW FILTERING", (today_str,)
            )})
        except Exception:
            device_ids = []

    # Build set of partition dates to touch between cutoff.date() and today
    date_strs = set()
    d = cutoff.date()
    while d <= now.date():
        date_strs.add(d.strftime('%Y-%m-%d'))
        d = d + timedelta(days=1)

    # device_metrics_raw: delete where timestamp < cutoff within each partition
    for device_id in device_ids:
        for dstr in date_strs:
            try:
                session.execute(
                    "DELETE FROM device_metrics_raw WHERE device_id=%s AND date=%s AND timestamp < %s",
                    (device_id, dstr, cutoff)
                )
            except Exception as e:
                print(f"    Warning deleting raw for {device_id}@{dstr}: {e}")
    print("  - device_metrics_raw: deleted rows older than cutoff (per-device partitions)")

    # device_state_snapshots: delete where snapshot_time < cutoff
    for device_id in device_ids:
        for dstr in date_strs:
            try:
                session.execute(
                    "DELETE FROM device_state_snapshots WHERE device_id=%s AND date=%s AND snapshot_time < %s",
                    (device_id, dstr, cutoff)
                )
            except Exception as e:
                print(f"    Warning deleting snapshots for {device_id}@{dstr}: {e}")
    print("  - device_state_snapshots: deleted rows older than cutoff (per-device partitions)")

    # metric_aggregation_buffer: delete old windows per device
    for device_id in device_ids:
        try:
            session.execute(
                "DELETE FROM metric_aggregation_buffer WHERE device_id=%s AND window_start <= %s",
                (device_id, cutoff)
            )
        except Exception as e:
            print(f"    Warning deleting buffer for {device_id}: {e}")
    print("  - metric_aggregation_buffer: deleted windows older than cutoff")

    print("\n✅ Windowed cleanup complete! (profiles preserved)")

print("\n📝 Next steps:")
print("  - Let producer/consumer run to repopulate recent data if needed")
print("  - Rebuild profiles only if you purposely cleared them (hard reset with --include-profiles)")

cluster.shutdown()
