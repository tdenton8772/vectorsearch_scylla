#!/usr/bin/env python3
"""
Path 3: Vector Search Anomaly Detection

Continuously polls for new snapshots and runs ANN queries to detect
uncommon patterns using ScyllaDB's vector index.

Workflow:
1. Maintain watermark (last processed timestamp)
2. Query snapshots newer than watermark
3. For each snapshot: Run ANN query to find similar snapshots
4. Filter results: same device, not anomalous, within 24h
5. If < 5 similar snapshots found → Path 3 anomaly
"""

import os
import time
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
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

# Path 3 thresholds
PATH3_MIN_MATCHES = 5
PATH3_SIMILARITY_THRESHOLD = 0.90
POLL_INTERVAL = 10  # seconds

# Warmup guard: require enough recent snapshots before judging anomalies
WARMUP_MIN_TOTAL = 30
WARMUP_LOOKBACK_HOURS = 24


def connect_scylla():
    """Connect to ScyllaDB."""
    auth_provider = PlainTextAuthProvider(
        username=scylla_username,
        password=scylla_password
    )
    cluster = Cluster(scylla_hosts, port=scylla_port, auth_provider=auth_provider)
    return cluster.connect(scylla_keyspace)


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    va = np.array(a)
    vb = np.array(b)
    denom = (np.linalg.norm(va) * np.linalg.norm(vb))
    if denom == 0:
        return 0.0
    return float(np.dot(va, vb) / denom)


def get_new_snapshots(session, last_timestamp: datetime, device_ids: List[str]) -> List:
    """
    Get all snapshots newer than the watermark timestamp.
    Query by (device_id, date) partition key and filter timestamp client-side.
    """
    now = datetime.now(timezone.utc)
    today = now.strftime('%Y-%m-%d')
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Query by partition key (device_id, date) - no ALLOW FILTERING needed
    query = """
        SELECT device_id, date, snapshot_time, embedding, device_type
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s
    """
    
    snapshots = []
    for device_id in device_ids:
        for date_str in [today, yesterday]:
            result = session.execute(query, (device_id, date_str))
            # Filter timestamp on client side
            for row in result:
                # Handle timezone-aware comparison
                snapshot_ts = row.snapshot_time
                if snapshot_ts.tzinfo is None:
                    snapshot_ts = snapshot_ts.replace(tzinfo=timezone.utc)
                if snapshot_ts > last_timestamp:
                    snapshots.append(row)
    
    # Sort by timestamp
    return sorted(snapshots, key=lambda s: s.snapshot_time)


def run_vector_search_path3(
    session,
    snapshot,
    min_matches: int = PATH3_MIN_MATCHES,
    similarity_threshold: float = PATH3_SIMILARITY_THRESHOLD
) -> Dict:
    """
    Run Path 3 vector search for a single snapshot.
    
    Returns dict with:
        - match_count: Number of similar normal snapshots found
        - is_anomalous: True if < min_matches
    """
    if not snapshot.embedding or len(snapshot.embedding) == 0:
        return {'match_count': 0, 'is_anomalous': True, 'reason': 'No embedding'}
    
    device_id = snapshot.device_id
    cutoff_time = snapshot.snapshot_time - timedelta(days=1)
    
    try:
        # ANN query: Get top 100 nearest neighbors
        ann_query = """
            SELECT device_id, snapshot_time, embedding, is_anomalous
            FROM device_state_snapshots
            ORDER BY embedding ANN OF %s
            LIMIT 100
        """
        import time
        ann_start = time.time()
        ann_results = list(session.execute(ann_query, (snapshot.embedding,)))
        ann_latency_ms = (time.time() - ann_start) * 1000
        
        # Filter in Python:
        # 1. Same device_id
        # 2. NOT anomalous (we want normal patterns)
        # 3. Within last 24 hours
        # 4. Not the current snapshot itself
        matches = []
        for result in ann_results:
            # Filter by device_id
            if result.device_id != device_id:
                continue
            
            # Filter out anomalous snapshots
            if result.is_anomalous:
                continue
            
            # Filter by time (within 24h)
            if result.snapshot_time < cutoff_time:
                continue
            
            # Don't match itself
            if result.snapshot_time == snapshot.snapshot_time:
                continue
            
            # Check similarity threshold
            sim = cosine_similarity(snapshot.embedding, result.embedding)
            if sim >= similarity_threshold:
                matches.append({
                    'timestamp': result.snapshot_time,
                    'similarity': sim
                })
        
        match_count = len(matches)
        is_anomalous = match_count < min_matches
        
        return {
            'match_count': match_count,
            'is_anomalous': is_anomalous,
            'reason': f"Only {match_count} similar normal snapshots found (need {min_matches}+)",
            'ann_latency_ms': ann_latency_ms,
            'ann_results_count': len(ann_results)
        }
        
    except Exception as e:
        print(f"  ⚠️  ANN query failed: {e}")
        return {'match_count': 0, 'is_anomalous': True, 'reason': f'ANN query error: {e}'}


def mark_path3_anomaly(session, snapshot, reason: str, ann_latency_ms: float = 0.0, match_count: int = 0, ann_results_count: int = 0):
    """Mark snapshot as anomalous due to Path 3 and record an anomaly_event."""
    import uuid
    date_str = snapshot.snapshot_time.strftime('%Y-%m-%d')

    # Update snapshot flag
    session.execute(
        """
        UPDATE device_state_snapshots
        SET is_anomalous = true
        WHERE device_id = %s AND date = %s AND snapshot_time = %s
        """,
        (snapshot.device_id, date_str, snapshot.snapshot_time)
    )

    # Record anomaly event for UI tooltips
    insert_event = """
        INSERT INTO anomaly_events
        (device_id, date, anomaly_id, device_type, detected_at, snapshot_time,
         anomaly_score, anomaly_type, metrics_snapshot, resolution_status,
         path1_rules_triggered, path2_fingerprint_triggered, path3_vector_triggered, detection_details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    metrics_snapshot = {
        'similar_normal_count': float(match_count),
        'ann_latency_ms': float(ann_latency_ms),
        'neighbors_checked': float(ann_results_count),
        'similarity_threshold': float(PATH3_SIMILARITY_THRESHOLD)
    }
    session.execute(
        insert_event,
        (
            snapshot.device_id,
            date_str,
            uuid.uuid1(),
            getattr(snapshot, 'device_type', None),
            snapshot.snapshot_time,
            snapshot.snapshot_time,
            1.0,  # Path 3-only score (no profile similarity available here)
            'vector_search',
            metrics_snapshot,
            'open',
            False,
            False,
            True,
            f"PATH 3 (Vector): {reason}"
        )
    )


def get_all_devices(session) -> List[str]:
    """Get list of all devices with profiles."""
    result = session.execute("SELECT device_id FROM device_profiles")
    return [row.device_id for row in result]


def count_recent_total(session, device_id: str, since_time: datetime) -> int:
    """Count recent snapshots (any status) for device since since_time.
    Query by (device_id, date) partitions and filter in client (no ALLOW FILTERING).
    """
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    query = """
        SELECT snapshot_time
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s
    """
    count = 0
    for date_str in [today, yesterday]:
        for row in session.execute(query, (device_id, date_str)):
            ts = row.snapshot_time
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= since_time:
                count += 1
    return count


def _watermark_path() -> str:
    os.makedirs('logs', exist_ok=True)
    return os.path.join('logs', 'path3_watermark.txt')


def load_watermark() -> Optional[datetime]:
    try:
        path = _watermark_path()
        if os.path.exists(path):
            with open(path, 'r') as f:
                txt = f.read().strip()
                if txt:
                    dt = datetime.fromisoformat(txt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
    except Exception:
        pass
    return None


def save_watermark(dt: datetime) -> None:
    try:
        with open(_watermark_path(), 'w') as f:
            f.write(dt.isoformat())
    except Exception:
        pass


def main():
    print("=" * 70)
    print("🔍 PATH 3: Vector Search Anomaly Detection")
    print("=" * 70)
    print(f"\nPolling interval: {POLL_INTERVAL}s")
    print(f"Min matches: {PATH3_MIN_MATCHES}")
    print(f"Similarity threshold: {PATH3_SIMILARITY_THRESHOLD}")
    print(f"\nFilters applied to ANN results:")
    print(f"  - Same device_id")
    print(f"  - is_anomalous = false (normal patterns only)")
    print(f"  - Within last 24 hours")
    
    session = connect_scylla()
    print("\n✅ Connected to ScyllaDB")
    
    # Get list of devices to monitor
    device_ids = get_all_devices(session)
    print(f"🔍 Monitoring {len(device_ids)} device(s): {', '.join(device_ids)}")
    
    # Initialize watermark
    persisted = load_watermark()
    if persisted is not None:
        last_processed = persisted
        print(f"\n🕐 Watermark loaded: {last_processed.isoformat()}")
    else:
        # No prior state → look back 5 minutes
        last_processed = datetime.now(timezone.utc) - timedelta(minutes=5)
        print(f"\n🕐 Watermark initialized (5m lookback): {last_processed.isoformat()}")
    print("\n🔄 Polling for new snapshots...\n")
    
    try:
        while True:
            # Get new snapshots since last check
            new_snapshots = get_new_snapshots(session, last_processed, device_ids)
            
            if new_snapshots:
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Found {len(new_snapshots)} new snapshot(s)")
                
                path3_anomalies = 0
                for snapshot in new_snapshots:
                    # Warmup guard: require enough recent total snapshots to make a decision
                    warmup_since = datetime.now(timezone.utc) - timedelta(hours=WARMUP_LOOKBACK_HOURS)
                    total = count_recent_total(session, snapshot.device_id, warmup_since)
                    if total < WARMUP_MIN_TOTAL:
                        print(
                            f"  ⏳ [PATH 3] {snapshot.device_id} @ {snapshot.snapshot_time.strftime('%H:%M:%S')} - "
                            f"warming up: {total} snapshots (< {WARMUP_MIN_TOTAL}); skipping"
                        )
                    else:
                        # Run Path 3 vector search
                        result = run_vector_search_path3(session, snapshot)
                        latency = result.get('ann_latency_ms', 0)
                    if result['is_anomalous']:
                        print(f"  🚨 [PATH 3] {snapshot.device_id} @ {snapshot.snapshot_time.strftime('%H:%M:%S')} - {result['reason']} [ANN: {latency:.1f}ms]")
                        mark_path3_anomaly(
                            session,
                            snapshot,
                            reason=result['reason'],
                            ann_latency_ms=latency,
                            match_count=result.get('match_count', 0),
                            ann_results_count=result.get('ann_results_count', 0)
                        )
                        path3_anomalies += 1
                    else:
                        print(f"  ✅ [PATH 3] {snapshot.device_id} @ {snapshot.snapshot_time.strftime('%H:%M:%S')} - {result['match_count']} matches [ANN: {latency:.1f}ms]")
                    
                    # Update watermark to this snapshot's timestamp
                    snapshot_ts = snapshot.snapshot_time
                    if snapshot_ts.tzinfo is None:
                        snapshot_ts = snapshot_ts.replace(tzinfo=timezone.utc)
                    if snapshot_ts > last_processed:
                        last_processed = snapshot_ts
                
                if path3_anomalies > 0:
                    print(f"  → {path3_anomalies} Path 3 anomal{'y' if path3_anomalies == 1 else 'ies'} detected")

                # Persist watermark after each batch
                save_watermark(last_processed)
            
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n✋ Stopped by user")
    finally:
        session.cluster.shutdown()


if __name__ == '__main__':
    main()
