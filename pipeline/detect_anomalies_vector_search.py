#!/usr/bin/env python3
"""
Detect anomalies using ScyllaDB Vector Search (ANN queries).

This version leverages ScyllaDB's vector index to find similar snapshots,
demonstrating the actual vector search capabilities rather than computing
similarity in Python.
"""

import argparse
import os
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
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

# Anomaly detection thresholds - 3-path approach
PROFILE_SIMILARITY_THRESHOLD = 0.75  # Path 2: Profile fingerprint similarity (relaxed)
PATH3_MIN_MATCHES = 5  # Path 3: Min similar snapshots from same device (relaxed)
PATH3_SIMILARITY_THRESHOLD = 0.75  # Path 3: Cosine similarity threshold for matching (relaxed)
OUTLIER_SIGMA_THRESHOLD = 6.0  # Path 1: Z-score for statistical outliers (relaxed)
OUTLIER_COUNT_THRESHOLD = 4  # Path 1: Min outlier metrics to flag (relaxed)


def connect_scylla():
    """Connect to ScyllaDB."""
    auth_provider = PlainTextAuthProvider(
        username=scylla_username,
        password=scylla_password
    )
    
    cluster = Cluster(
        scylla_hosts,
        port=scylla_port,
        auth_provider=auth_provider
    )
    
    return cluster.connect(scylla_keyspace)


def get_device_profile(session, device_id: str) -> Optional[Dict]:
    """Get device profile from ScyllaDB."""
    query = """
        SELECT profile_embedding, metric_stats
        FROM device_profiles
        WHERE device_id = %s
    """
    
    result = session.execute(query, (device_id,))
    row = result.one()
    
    if not row:
        return None
    
    return {
        'profile_embedding': row.profile_embedding,
        'metric_stats': row.metric_stats,
    }


def get_unanalyzed_snapshots(session, device_id: str, minutes_back: int = 1) -> List:
    """Get snapshots that haven't been analyzed by Path 3 yet.
    
    We need a separate way to track Path 3 analysis since the consumer
    already sets is_anomalous based on Path 1 & 2.
    
    Strategy: Get recent snapshots and check if they have corresponding
    entries in anomaly_events with path3_vector_triggered set.
    
    Args:
        minutes_back: How many minutes back to check for new snapshots
    """
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=minutes_back)
    
    # Get dates to query
    today = now.strftime('%Y-%m-%d')
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    
    query = """
        SELECT device_id, snapshot_time, embedding, metrics, is_anomalous, device_type, anomaly_score
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s AND snapshot_time >= %s
        ORDER BY snapshot_time DESC
    """
    
    snapshots = []
    for date_str in [today, yesterday]:
        result = session.execute(query, (device_id, date_str, start_time))
        snapshots.extend(list(result))
    
    return sorted(snapshots, key=lambda s: s.snapshot_time, reverse=True)


def find_similar_snapshots_vector_search(
    session, 
    query_embedding: List[float],
    device_id: str,
    date: str,
    limit: int = 50  # Request more since we'll filter
) -> List[Dict]:
    """
    Use ScyllaDB Vector Search to find similar snapshots.
    
    IMPORTANT: ScyllaDB ANN queries don't support WHERE clause filtering,
    so we query all devices and filter by device_id afterward.
    
    This performs an ANN (Approximate Nearest Neighbor) query using
    the vector index, demonstrating actual vector search capabilities.
    """
    # Vector search query - NO WHERE clause (ScyllaDB ANN limitation)
    # ScyllaDB's ANN query doesn't support WHERE filtering at all
    # We query globally and filter results in Python
    ann_query = """
        SELECT device_id, snapshot_time, embedding, metrics, is_anomalous
        FROM device_state_snapshots
        ORDER BY embedding ANN OF %s
        LIMIT %s
    """
    
    result = session.execute(
        ann_query,
        (query_embedding, limit)
    )
    
    # Filter results to only include the target device_id
    similar_snapshots = []
    for row in result:
        if row.device_id == device_id:
            similar_snapshots.append({
                'device_id': row.device_id,
                'snapshot_time': row.snapshot_time,
                'embedding': row.embedding,
                'metrics': row.metrics,
                'is_anomalous': row.is_anomalous
            })
    
    return similar_snapshots[:10]  # Return top 10 for target device


def compute_cosine_similarity(embedding1: list, embedding2: list) -> float:
    """
    Compute COSINE similarity between two embeddings.
    
    Note: We still need this to score the results returned by vector search.
    """
    vec1 = np.array(embedding1)
    vec2 = np.array(embedding2)
    
    dot_product = np.dot(vec1, vec2)
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    similarity = dot_product / (norm1 * norm2)
    return float(similarity)


def check_metric_outliers(snapshot_metrics: Dict, profile_stats: Dict,
                          sigma_threshold: float = 3.0) -> Dict:
    """
    Check for metric values that are statistical outliers.
    """
    outliers = {}
    
    for metric_name, value in snapshot_metrics.items():
        if metric_name not in profile_stats:
            continue
        
        stats = profile_stats[metric_name]
        mean = stats['mean']
        std = stats['std']
        
        if std == 0:
            continue
        
        z_score = abs((value - mean) / std)
        
        if z_score > sigma_threshold:
            outliers[metric_name] = {
                'current_value': value,
                'mean': mean,
                'std': std,
                'z_score': z_score,
                'deviation_percent': ((value - mean) / mean * 100) if mean != 0 else 0
            }
    
    return outliers


def record_anomaly(session, device_id: str, device_type: str, snapshot_time: datetime,
                   similarity_score: float, outlier_metrics: Dict,
                   similar_count: int = 0,
                   path1_triggered: bool = False,
                   path2_triggered: bool = False,
                   path3_triggered: bool = False,
                   anomaly_reasons: list = None):
    """Record an anomaly event in ScyllaDB."""
    import uuid
    
    date_str = snapshot_time.strftime('%Y-%m-%d')
    anomaly_score = 1.0 - similarity_score
    
    # Prepare metrics snapshot - must be map<text, double>
    metrics_snapshot = {
        'similarity_score': float(similarity_score),
        'outlier_count': float(len(outlier_metrics)),
        'similar_normal_count': float(similar_count)
    }
    
    # Add outlier details as doubles
    for metric_name, outlier_info in outlier_metrics.items():
        metrics_snapshot[f'outlier_{metric_name}'] = float(outlier_info['z_score'])
    
    # Build detection details string
    detection_details = '; '.join(anomaly_reasons) if anomaly_reasons else 'Unknown'
    
    # Insert into anomaly_events with detection path tracking
    insert_event = """
        INSERT INTO anomaly_events
        (device_id, date, anomaly_id, device_type, detected_at, snapshot_time,
         anomaly_score, anomaly_type, metrics_snapshot, resolution_status,
         path1_rules_triggered, path2_fingerprint_triggered, path3_vector_triggered, detection_details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    session.execute(
        insert_event,
        (device_id, date_str, uuid.uuid1(), device_type, snapshot_time, snapshot_time,
         anomaly_score, 'multi_path', metrics_snapshot, 'open',
         path1_triggered, path2_triggered, path3_triggered, detection_details)
    )
    
    # Update device statistics counter
    session.execute(
        "UPDATE device_statistics SET anomaly_count = anomaly_count + 1 WHERE device_id = %s",
        (device_id,)
    )
    
    # Mark snapshot as anomalous
    update_snapshot = """
        UPDATE device_state_snapshots
        SET is_anomalous = true, anomaly_score = %s
        WHERE device_id = %s AND date = %s AND snapshot_time = %s
    """
    
    session.execute(
        update_snapshot,
        (anomaly_score, device_id, date_str, snapshot_time)
    )


def analyze_snapshot_path3_only(
    session,
    device_id: str,
    device_type: str,
    snapshot,
    path3_min_matches: int = PATH3_MIN_MATCHES
) -> Dict:
    """
    Run ONLY Path 3 (Vector Search) analysis.
    Path 1 & 2 are already done by the consumer.
    """
    # PATH 3: Vector Search with ANN Query
    path3_match_count = 0
    
    if snapshot.embedding and len(snapshot.embedding) > 0:
        try:
            # Query top 100 nearest neighbors globally
            ann_query = """
                SELECT device_id, snapshot_time, embedding
                FROM device_state_snapshots
                ORDER BY embedding ANN OF %s
                LIMIT 100
            """
            ann_results = list(session.execute(ann_query, (snapshot.embedding,)))
            
            # Filter: same device + high similarity + not current snapshot
            similar_from_device = []
            for result in ann_results:
                if result.device_id == device_id and result.snapshot_time != snapshot.snapshot_time:
                    # Compute similarity
                    sim = compute_cosine_similarity(snapshot.embedding, result.embedding)
                    if sim >= PATH3_SIMILARITY_THRESHOLD:
                        similar_from_device.append({'similarity': sim, 'time': result.snapshot_time})
            
            path3_match_count = len(similar_from_device)
            
        except Exception as e:
            print(f"      Warning: ANN query failed: {e}")
            path3_match_count = 0
    
    path3_triggered = path3_match_count < path3_min_matches
    
    result = {
        'device_id': device_id,
        'snapshot_time': snapshot.snapshot_time,
        'path3_match_count': path3_match_count,
        'path3_triggered': path3_triggered,
        'anomaly_reasons': []
    }
    
    if path3_triggered:
        result['anomaly_reasons'].append(
            f"PATH 3 (Vector): Only {path3_match_count} similar snapshots found (need {path3_min_matches}+, similarity >= {PATH3_SIMILARITY_THRESHOLD})"
        )
    
    return result


def update_snapshot_path3(session, device_id: str, snapshot_time: datetime, result: Dict):
    """Update snapshot with Path 3 detection result."""
    date_str = snapshot_time.strftime('%Y-%m-%d')
    
    # Mark as anomalous if Path 3 triggered
    session.execute(
        """
        UPDATE device_state_snapshots
        SET is_anomalous = true
        WHERE device_id = %s AND date = %s AND snapshot_time = %s
        """,
        (device_id, date_str, snapshot_time)
    )


def analyze_snapshot_with_vector_search(
    session,
    device_id: str,
    device_type: str,
    snapshot,
    profile: Dict,
    profile_threshold: float = PROFILE_SIMILARITY_THRESHOLD,
    path3_min_matches: int = PATH3_MIN_MATCHES,
    sigma_threshold: float = OUTLIER_SIGMA_THRESHOLD,
    outlier_count: int = OUTLIER_COUNT_THRESHOLD
) -> Dict:
    """
    3-Path Anomaly Detection:
    
    Path 1: Rules Engine - Statistical outliers (Z-score based)
    Path 2: Profile Fingerprint - Similarity to device's historical baseline
    Path 3: Peer Comparison - ANN query to see if pattern is common for THIS device
    """
    date_str = snapshot.snapshot_time.strftime('%Y-%m-%d')
    
    # PATH 3: Vector Search with Post-Query Filtering
    # 1. Run ANN to get 100 nearest neighbors (globally, no WHERE clause)
    # 2. Filter by device_id and similarity threshold in Python
    # 3. If >= 10 matches: common pattern for this device = NORMAL
    # 4. If < 10 matches: uncommon pattern = ANOMALY
    
    path3_match_count = 0  # Default if ANN fails
    
    if snapshot.embedding and len(snapshot.embedding) > 0:
        try:
            # Query top 100 nearest neighbors globally
            ann_query = """
                SELECT device_id, snapshot_time, embedding
                FROM device_state_snapshots
                ORDER BY embedding ANN OF %s
                LIMIT 100
            """
            ann_results = list(session.execute(ann_query, (snapshot.embedding,)))
            
            # Filter: same device + high similarity + not current snapshot
            similar_from_device = []
            for result in ann_results:
                if result.device_id == device_id and result.snapshot_time != snapshot.snapshot_time:
                    # Compute similarity
                    sim = compute_cosine_similarity(snapshot.embedding, result.embedding)
                    if sim >= PATH3_SIMILARITY_THRESHOLD:
                        similar_from_device.append({'similarity': sim, 'time': result.snapshot_time})
            
            path3_match_count = len(similar_from_device)
            
        except Exception as e:
            print(f"      Warning: ANN query failed: {e}")
            path3_match_count = 0
    
    # PATH 2: Profile Fingerprint Similarity
    profile_similarity = compute_cosine_similarity(
        snapshot.embedding,
        profile['profile_embedding']
    )
    
    # PATH 1: Statistical Outlier Detection
    outlier_metrics = check_metric_outliers(
        snapshot.metrics,
        profile['metric_stats'],
        sigma_threshold=sigma_threshold
    )
    
    # 3-PATH DECISION LOGIC
    # Anomalous if ANY path triggers:
    path1_triggered = len(outlier_metrics) >= outlier_count
    path2_triggered = profile_similarity < profile_threshold
    path3_triggered = path3_match_count < path3_min_matches
    
    is_anomalous = path1_triggered or path2_triggered or path3_triggered
    
    result = {
        'device_id': device_id,
        'snapshot_time': snapshot.snapshot_time,
        'profile_similarity': profile_similarity,
        'path3_match_count': path3_match_count,
        'outlier_metrics': outlier_metrics,
        'is_anomalous': is_anomalous,
        'anomaly_reasons': [],
        'path1_triggered': path1_triggered,
        'path2_triggered': path2_triggered,
        'path3_triggered': path3_triggered,
    }
    
    # Build anomaly reasons based on which paths triggered
    if path1_triggered:
        outlier_names = list(outlier_metrics.keys())
        z_scores = [outlier_metrics[m]['z_score'] for m in outlier_names]
        result['anomaly_reasons'].append(
            f"PATH 1 (Rules): {len(outlier_metrics)} outliers - {', '.join(outlier_names[:3])} (Z: {', '.join(f'{z:.1f}' for z in z_scores[:3])})"
        )
    
    if path2_triggered:
        result['anomaly_reasons'].append(
            f"PATH 2 (Profile): Low similarity {profile_similarity:.3f} < {profile_threshold}"
        )
    
    if path3_triggered:
        result['anomaly_reasons'].append(
            f"PATH 3 (Vector): Uncommon pattern - only {path3_match_count} similar snapshots from this device (need {path3_min_matches}+, similarity >= {PATH3_SIMILARITY_THRESHOLD})"
        )
    
    return result


def detect_anomalies_for_device(
    session,
    device_id: str,
    profile_threshold: float = PROFILE_SIMILARITY_THRESHOLD,
    path3_min_matches: int = PATH3_MIN_MATCHES,
    record_events: bool = True,
    minutes_back: int = 1
) -> List[Dict]:
    """
    Run Path 3 (Vector Search) on recent unanalyzed snapshots.
    
    Args:
        minutes_back: How many minutes of snapshots to process
    
    Returns:
        List of results for each snapshot analyzed
    """
    # Get device profile
    profile = get_device_profile(session, device_id)
    if not profile:
        return []
    
    # Get recent snapshots that need Path 3 analysis
    snapshots = get_unanalyzed_snapshots(session, device_id, minutes_back=minutes_back)
    
    if not snapshots:
        return []
    
    results = []
    print(f"  🔍 {device_id}: Analyzing {len(snapshots)} snapshot(s) for Path 3")
    
    for snapshot in snapshots:
        if not snapshot.embedding:
            continue
        
        device_type = snapshot.device_type
        
        # ONLY run Path 3 vector search (Path 1 & 2 already done by consumer)
        result = analyze_snapshot_path3_only(
            session,
            device_id,
            device_type,
            snapshot,
            path3_min_matches=path3_min_matches
        )
        
        # If Path 3 detects anomaly and snapshot wasn't already marked, update it
        if result['path3_triggered']:
            print(f"     🚨 PATH 3 ANOMALY: {snapshot.snapshot_time} - {result['anomaly_reasons'][0]}")
            
            if record_events:
                # Update snapshot with Path 3 result
                update_snapshot_path3(session, device_id, snapshot.snapshot_time, result)
        
        results.append(result)
    
    return results


def get_all_profiled_devices(session) -> List[str]:
    """Get list of all devices that have profiles."""
    query = "SELECT device_id FROM device_profiles"
    result = session.execute(query)
    return [row.device_id for row in result]


def main():
    parser = argparse.ArgumentParser(
        description='Detect anomalies using ScyllaDB Vector Search (ANN queries)'
    )
    parser.add_argument(
        '--devices',
        nargs='+',
        default=None,
        help='Device IDs to analyze (default: auto-discover from device_profiles table)'
    )
    parser.add_argument(
        '--only-new',
        action='store_true',
        help='Only analyze snapshots not yet marked (for continuous mode)'
    )
    parser.add_argument(
        '--profile-threshold',
        type=float,
        default=PROFILE_SIMILARITY_THRESHOLD,
        help=f'Profile similarity threshold (Path 2) (default: {PROFILE_SIMILARITY_THRESHOLD})'
    )
    parser.add_argument(
        '--path3-min-matches',
        type=int,
        default=PATH3_MIN_MATCHES,
        help=f'Min similar snapshots from same device (Path 3) (default: {PATH3_MIN_MATCHES})'
    )
    parser.add_argument(
        '--sigma-threshold',
        type=float,
        default=OUTLIER_SIGMA_THRESHOLD,
        help=f'Standard deviations for metric outlier detection (Path 1) (default: {OUTLIER_SIGMA_THRESHOLD})'
    )
    parser.add_argument(
        '--no-record',
        action='store_true',
        help='Do not record anomalies in database (dry run)'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuously, checking every 30 seconds'
    )
    
    args = parser.parse_args()
    
    # Connect to ScyllaDB first to discover devices if needed
    session = connect_scylla()
    
    # Auto-discover devices if not specified
    if args.devices is None:
        args.devices = get_all_profiled_devices(session)
        print("🔍 Auto-discovered devices from profiles table")
    
    print("=" * 70)
    print("🔍 PATH 3 (Vector Search) Anomaly Detection")
    print("=" * 70)
    print(f"\nDevices: {', '.join(args.devices)}")
    print(f"Mode: Analyze recent snapshots (last 1 minute)")
    print(f"\nPath 3 (Vector): Similar snapshots from device < {args.path3_min_matches} (similarity >= {PATH3_SIMILARITY_THRESHOLD})")
    print(f"\nRecord events: {not args.no_record}")
    print(f"\nNOTE: Path 1 & 2 are handled by the consumer in real-time")
    print("\n✅ Connected to ScyllaDB")
    
    def run_detection():
        total_path3_anomalies = 0
        total_snapshots_checked = 0
        
        for device_id in args.devices:
            results = detect_anomalies_for_device(
                session,
                device_id,
                path3_min_matches=args.path3_min_matches,
                record_events=not args.no_record,
                minutes_back=1  # Check last minute of snapshots
            )
            
            if results:
                total_snapshots_checked += len(results)
                path3_anomalies = sum(1 for r in results if r['path3_triggered'])
                total_path3_anomalies += path3_anomalies
        
        # Summary
        if total_snapshots_checked > 0:
            timestamp = datetime.now(timezone.utc).strftime('%H:%M:%S')
            print(f"\n[{timestamp}] Checked {total_snapshots_checked} snapshot(s) → {total_path3_anomalies} Path 3 anomal{'y' if total_path3_anomalies == 1 else 'ies'}")
        
        return total_path3_anomalies
    
    if args.continuous:
        import time
        print("\n🔄 Running continuously (Ctrl+C to stop)...\n")
        try:
            while True:
                run_detection()
                time.sleep(30)
        except KeyboardInterrupt:
            print("\n\n✋ Stopped by user")
    else:
        run_detection()
    
    session.cluster.shutdown()


if __name__ == '__main__':
    main()
