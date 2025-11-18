#!/usr/bin/env python3
"""
Create ScyllaDB schema for IoT device metrics and embeddings.

Tables:
1. device_metrics_raw - Raw time series data (one metric per row)
2. metric_aggregation_buffer - Temporary buffer for windowed aggregation (replaces Redis)
3. device_state_snapshots - Aggregated device state with embeddings
4. device_profiles - Baseline normal behavior profiles
5. device_statistics - Counter table for metrics
6. anomaly_events - Detected anomalies
7. recent_device_states - Materialized view for recent states
"""

import os
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

# ScyllaDB connection from environment
SCYLLA_HOSTS = os.getenv('SCYLLA_HOSTS', 'localhost').split(',')
SCYLLA_PORT = int(os.getenv('SCYLLA_PORT', '9042'))
SCYLLA_USERNAME = os.getenv('SCYLLA_USERNAME')
SCYLLA_PASSWORD = os.getenv('SCYLLA_PASSWORD')
SCYLLA_KEYSPACE = os.getenv('SCYLLA_KEYSPACE', 'iot_monitoring')

# Vector dimensions
EMBEDDING_DIM = 384  # all-minilm:l6-v2 output dimension


def create_schema():
    """Create keyspace and tables for IoT monitoring."""
    
    # Connect to ScyllaDB
    if SCYLLA_USERNAME and SCYLLA_PASSWORD:
        auth_provider = PlainTextAuthProvider(
            username=SCYLLA_USERNAME,
            password=SCYLLA_PASSWORD
        )
        cluster = Cluster(
            SCYLLA_HOSTS,
            port=SCYLLA_PORT,
            auth_provider=auth_provider
        )
    else:
        cluster = Cluster(SCYLLA_HOSTS, port=SCYLLA_PORT)
    
    session = cluster.connect()
    
    print(f"Creating keyspace: {SCYLLA_KEYSPACE}")
    
    # Create keyspace
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {SCYLLA_KEYSPACE}
        WITH replication = {{
            'class': 'NetworkTopologyStrategy',
            'replication_factor': 3
        }}
        AND tablets = {{ 'enabled': false }}
    """)
    
    session.set_keyspace(SCYLLA_KEYSPACE)
    
    # 1. Raw metrics table - stores every individual metric
    print("Creating table: device_metrics_raw")
    session.execute("""
        CREATE TABLE IF NOT EXISTS device_metrics_raw (
            device_id text,
            date text,                    -- YYYY-MM-DD for bucketing
            timestamp timestamp,
            metric_name text,
            metric_value double,
            unit text,
            device_type text static,      -- Static: same for all metrics of this device
            location text static,         -- Static: same for all metrics of this device
            building_id text static,      -- Static: same for all metrics of this device
            PRIMARY KEY ((device_id, date), timestamp, metric_name)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, metric_name ASC)
          AND compression = {'sstable_compression': 'LZ4Compressor'}
          AND compaction = {'class': 'IncrementalCompactionStrategy'}
          AND gc_grace_seconds = 86400
          AND default_time_to_live = 2592000
    """)
    
    print("Creating index on device_type...")
    session.execute("""
        CREATE INDEX IF NOT EXISTS device_type_idx 
        ON device_metrics_raw (device_type)
    """)
    
    # 2. Metric aggregation buffer - temporary storage for windowed aggregation
    # Replaces Redis for metric buffering
    print("Creating table: metric_aggregation_buffer")
    session.execute("""
        CREATE TABLE IF NOT EXISTS metric_aggregation_buffer (
            device_id text,
            window_start timestamp,          -- Start of aggregation window
            metric_name text,
            metric_value double,
            device_type text static,
            location text static,
            building_id text static,
            last_updated timestamp,
            PRIMARY KEY ((device_id, window_start), metric_name)
        ) WITH CLUSTERING ORDER BY (metric_name ASC)
          AND compression = {'sstable_compression': 'LZ4Compressor'}
          AND default_time_to_live = 3600  -- Auto-expire after 1 hour
    """)
    
    # 3. Device state snapshots - aggregated state with embeddings
    print("Creating table: device_state_snapshots")
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS device_state_snapshots (
            device_id text,
            date text,                    -- YYYY-MM-DD for bucketing
            snapshot_time timestamp,
            device_type text static,      -- Static: same for all snapshots of this device
            location text static,         -- Static: same for all snapshots of this device
            building_id text static,      -- Static: same for all snapshots of this device
            metrics map<text, double>,    -- All metrics in one snapshot
            embedding vector<float, {EMBEDDING_DIM}>,
            embedding_method text,        -- 'ollama_text' or 'tabtransformer'
            anomaly_score double,
            is_anomalous boolean,
            PRIMARY KEY ((device_id, date), snapshot_time)
        ) WITH CLUSTERING ORDER BY (snapshot_time DESC)
          AND compression = {{'sstable_compression': 'LZ4Compressor'}}
          AND compaction = {{'class': 'IncrementalCompactionStrategy'}}
          AND gc_grace_seconds = 86400
          AND default_time_to_live = 7776000
    """)
    
    print("Creating vector index on embeddings...")
    try:
        session.execute(f"""
            CREATE CUSTOM INDEX IF NOT EXISTS device_state_embedding_idx
            ON device_state_snapshots (embedding)
            USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'
            WITH OPTIONS = {{
                'similarity_function': 'cosine'
            }}
        """)
    except Exception as e:
        print(f"Warning: Could not create SAI index, trying vector_index: {e}")
        session.execute(f"""
            CREATE CUSTOM INDEX IF NOT EXISTS device_state_embedding_idx
            ON device_state_snapshots (embedding)
            USING 'vector_index'
            WITH OPTIONS = {{
                'similarity_function': 'cosine'
            }}
        """)
    
    # 4. Device profiles - baseline normal behavior
    print("Creating table: device_profiles")
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS device_profiles (
            device_id text PRIMARY KEY,
            device_type text,
            location text,
            building_id text,
            profile_embedding vector<float, {EMBEDDING_DIM}>,
            profile_created_at timestamp,
            profile_updated_at timestamp,
            metric_stats map<text, frozen<map<text, double>>>,  -- {{metric: {{mean, std, min, max}}}}
            last_seen timestamp
        )
    """)
    
    # 5. Device statistics counters (separate table for counters)
    print("Creating table: device_statistics")
    session.execute("""
        CREATE TABLE IF NOT EXISTS device_statistics (
            device_id text PRIMARY KEY,
            total_snapshots counter,
            anomaly_count counter
        )
    """)
    
    # 6. Anomaly events - flagged anomalies with similar devices
    print("Creating table: anomaly_events")
    session.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_events (
            device_id text,
            date text,                      -- YYYY-MM-DD for bucketing
            anomaly_id timeuuid,
            device_type text,
            detected_at timestamp,
            snapshot_time timestamp,
            anomaly_score double,
            anomaly_type text,              -- 'deviation', 'outlier', 'pattern_change'
            metrics_snapshot map<text, double>,
            similar_device_ids list<text>,  -- Most similar normal devices
            similar_scores list<double>,    -- Similarity scores
            resolution_status text,         -- 'open', 'investigating', 'resolved', 'false_positive'
            notes text,
            PRIMARY KEY ((device_id, date), anomaly_id)
        ) WITH CLUSTERING ORDER BY (anomaly_id DESC)
          AND default_time_to_live = 7776000
    """)
    
    print("Creating index on device_id for anomalies...")
    session.execute("""
        CREATE INDEX IF NOT EXISTS anomaly_device_idx
        ON anomaly_events (device_id)
    """)
    
    print("Creating index on detected_at for anomalies...")
    session.execute("""
        CREATE INDEX IF NOT EXISTS anomaly_time_idx
        ON anomaly_events (detected_at)
    """)
    
    # 7. Create materialized view for recent device states (optional)
    # Note: MVs cannot include static columns - exclude device_type, location, building_id
    print("Creating materialized view: recent_device_states")
    session.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS recent_device_states AS
        SELECT device_id, snapshot_time, metrics, anomaly_score, is_anomalous, date
        FROM device_state_snapshots
        WHERE device_id IS NOT NULL
          AND date IS NOT NULL
          AND snapshot_time IS NOT NULL
        PRIMARY KEY (date, snapshot_time, device_id)
        WITH CLUSTERING ORDER BY (snapshot_time DESC, device_id ASC)
    """)
    
    print("\n✅ Schema created successfully!")
    print(f"\nKeyspace: {SCYLLA_KEYSPACE}")
    print("Tables:")
    print("  - device_metrics_raw (raw time series)")
    print("  - device_state_snapshots (aggregated with embeddings)")
    print("  - device_profiles (baseline profiles)")
    print("  - anomaly_events (detected anomalies)")
    print("  - recent_device_states (MV for queries)")
    
    cluster.shutdown()


def drop_schema():
    """Drop the keyspace (use with caution!)"""
    if SCYLLA_USERNAME and SCYLLA_PASSWORD:
        auth_provider = PlainTextAuthProvider(
            username=SCYLLA_USERNAME,
            password=SCYLLA_PASSWORD
        )
        cluster = Cluster(
            SCYLLA_HOSTS,
            port=SCYLLA_PORT,
            auth_provider=auth_provider
        )
    else:
        cluster = Cluster(SCYLLA_HOSTS, port=SCYLLA_PORT)
    
    session = cluster.connect()
    
    confirm = input(f"⚠️  Drop keyspace {SCYLLA_KEYSPACE}? This will delete all data! (yes/no): ")
    if confirm.lower() == 'yes':
        print(f"Dropping keyspace: {SCYLLA_KEYSPACE}")
        session.execute(f"DROP KEYSPACE IF EXISTS {SCYLLA_KEYSPACE}")
        print("✅ Keyspace dropped")
    else:
        print("Cancelled")
    
    cluster.shutdown()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage IoT monitoring schema')
    parser.add_argument(
        '--drop',
        action='store_true',
        help='Drop the keyspace (WARNING: deletes all data)'
    )
    
    args = parser.parse_args()
    
    if args.drop:
        drop_schema()
    else:
        create_schema()
