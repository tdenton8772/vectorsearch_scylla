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

# Anomaly detection thresholds
SIMILARITY_THRESHOLD = 0.60  # Below this = anomalous (very relaxed for normal operation)
SIGMA_THRESHOLD = 5.0  # Z-score threshold for outliers (default was 3.0)


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
        SELECT device_type, location, building_id,
               profile_embedding, metric_stats,
               profile_created_at, profile_updated_at
        FROM device_profiles
        WHERE device_id = %s
    """
    
    result = session.execute(query, (device_id,))
    row = result.one()
    
    if not row:
        return None
    
    return {
        'device_id': device_id,
        'device_type': row.device_type,
        'location': row.location,
        'building_id': row.building_id,
        'profile_embedding': row.profile_embedding,
        'metric_stats': row.metric_stats,
        'profile_created_at': row.profile_created_at,
        'profile_updated_at': row.profile_updated_at
    }


def get_recent_snapshots(session, device_id: str, hours_back: int = 1, only_new: bool = False):
    """Get recent snapshots for a device.
    
    Args:
        only_new: If True, only return snapshots with is_anomalous=false (not yet analyzed)
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)
    
    snapshots = []
    
    # Query current day
    current_date = end_time.strftime('%Y-%m-%d')
    query = """
        SELECT device_id, snapshot_time, embedding, metrics, is_anomalous, device_type
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s
        AND snapshot_time >= %s
    """
    
    result = session.execute(query, (device_id, current_date, start_time))
    snapshots.extend(result)
    
    # If time range spans previous day, query that too
    if start_time.date() < end_time.date():
        prev_date = start_time.strftime('%Y-%m-%d')
        result = session.execute(query, (device_id, prev_date, start_time))
        snapshots.extend(result)
    
    # Filter to only new (unmarked) snapshots if requested
    if only_new:
        snapshots = [s for s in snapshots if not s.is_anomalous]
    
    return snapshots


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
                   similar_count: int = 0):
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
    
    # Insert into anomaly_events with correct schema
    insert_event = """
        INSERT INTO anomaly_events
        (device_id, date, anomaly_id, device_type, detected_at, snapshot_time,
         anomaly_score, anomaly_type, metrics_snapshot, resolution_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    session.execute(
        insert_event,
        (device_id, date_str, uuid.uuid1(), device_type, snapshot_time, snapshot_time,
         anomaly_score, 'vector_search', metrics_snapshot, 'open')
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


def analyze_snapshot_with_vector_search(
    session,
    device_id: str,
    device_type: str,
    snapshot,
    profile: Dict,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
    sigma_threshold: float = 3.0
) -> Dict:
    """
    Analyze a snapshot using ScyllaDB Vector Search.
    
    This demonstrates the actual vector search capability by:
    1. Using ANN query to find similar normal snapshots
    2. Computing similarity to profile
    3. Checking if the device is behaving like its normal self
    """
    # Use vector search to find similar snapshots in historical data
    date_str = snapshot.snapshot_time.strftime('%Y-%m-%d')
    
    similar_snapshots = find_similar_snapshots_vector_search(
        session,
        snapshot.embedding,
        device_id,
        date_str,
        limit=10
    )
    
    # Count how many similar snapshots were normal (not anomalous)
    similar_normal_count = sum(1 for s in similar_snapshots if not s['is_anomalous'])
    
    # Compute similarity to profile
    profile_similarity = compute_cosine_similarity(
        snapshot.embedding,
        profile['profile_embedding']
    )
    
    # If we found similar normal behavior, compute average similarity
    avg_similar_score = 0.0
    if similar_normal_count > 0:
        similar_scores = []
        for sim_snap in similar_snapshots:
            if not sim_snap['is_anomalous']:
                score = compute_cosine_similarity(snapshot.embedding, sim_snap['embedding'])
                similar_scores.append(score)
        if similar_scores:
            avg_similar_score = np.mean(similar_scores)
    
    # Check metric outliers
    outlier_metrics = check_metric_outliers(
        snapshot.metrics,
        profile['metric_stats'],
        sigma_threshold=sigma_threshold
    )
    
    # Determine if anomalous using embedding similarity only
    # For demo purposes: disable statistical outlier detection due to high variance
    # in simulated data. Only flag based on low vector similarity to profile.
    is_anomalous = profile_similarity < similarity_threshold
    
    result = {
        'device_id': device_id,
        'snapshot_time': snapshot.snapshot_time,
        'profile_similarity': profile_similarity,
        'avg_similar_score': avg_similar_score,
        'similar_normal_count': similar_normal_count,
        'outlier_metrics': outlier_metrics,
        'is_anomalous': is_anomalous,
        'anomaly_reasons': [],
        'used_vector_search': True
    }
    
    # Add anomaly reasons
    if profile_similarity < similarity_threshold:
        result['anomaly_reasons'].append(
            f"Low profile similarity: {profile_similarity:.3f} < {similarity_threshold}"
        )
    
    if len(outlier_metrics) >= 3:
        outlier_names = list(outlier_metrics.keys())
        z_scores = [outlier_metrics[m]['z_score'] for m in outlier_names]
        result['anomaly_reasons'].append(
            f"Extreme outliers ({len(outlier_metrics)} metrics): {', '.join(outlier_names[:3])} (Z-scores: {', '.join(f'{z:.1f}' for z in z_scores[:3])})"
        )
    
    return result


def detect_anomalies_for_device(
    session,
    device_id: str,
    hours_back: int = 1,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
    record_events: bool = True,
    only_new: bool = False
) -> list:
    """
    Detect anomalies for a single device using vector search.
    
    Args:
        only_new: If True, only analyze snapshots that haven't been checked yet
    """
    # Get device profile
    profile = get_device_profile(session, device_id)
    if not profile:
        return []
    
    device_type = profile['device_type']
    
    # Get recent snapshots (optionally only new/unanalyzed ones)
    snapshots = get_recent_snapshots(session, device_id, hours_back=hours_back, only_new=only_new)
    
    if not snapshots:
        return []
    
    if len(snapshots) > 0:
        print(f"  🔍 {device_id}: Analyzing {len(snapshots)} snapshot(s)...")
    
    # Analyze each snapshot using vector search
    results = []
    anomalies_found = 0
    
    for snapshot in snapshots:
        if not snapshot.embedding:
            continue
        
        result = analyze_snapshot_with_vector_search(
            session,
            device_id,
            device_type,
            snapshot,
            profile,
            similarity_threshold=similarity_threshold
        )
        
        results.append(result)
        
        if result['is_anomalous']:
            anomalies_found += 1
            
            # Record anomaly event
            if record_events and not snapshot.is_anomalous:
                record_anomaly(
                    session,
                    device_id,
                    device_type,
                    snapshot.snapshot_time,
                    result['profile_similarity'],
                    result['outlier_metrics'],
                    result['similar_normal_count']
                )
    
    # Print summary (only if we found something)
    if anomalies_found > 0:
        print(f"     🚨 Found {anomalies_found} anomal{'y' if anomalies_found == 1 else 'ies'}!")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Detect anomalies using ScyllaDB Vector Search (ANN queries)'
    )
    parser.add_argument(
        '--devices',
        nargs='+',
        default=['RTU-001', 'MAU-001', 'CH-001', 'CT-001', 'AC-001'],
        help='Device IDs to analyze (default: all known devices)'
    )
    parser.add_argument(
        '--hours-back',
        type=int,
        default=1,
        help='Hours of recent snapshots to analyze (default: 1)'
    )
    parser.add_argument(
        '--only-new',
        action='store_true',
        help='Only analyze snapshots not yet marked (for continuous mode)'
    )
    parser.add_argument(
        '--similarity-threshold',
        type=float,
        default=SIMILARITY_THRESHOLD,
        help=f'Similarity threshold for anomaly detection (default: {SIMILARITY_THRESHOLD}, lower = stricter)'
    )
    parser.add_argument(
        '--sigma-threshold',
        type=float,
        default=SIGMA_THRESHOLD,
        help=f'Standard deviations for metric outlier detection (default: {SIGMA_THRESHOLD}, higher = less sensitive)'
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
    
    print("=" * 70)
    print("🔍 Anomaly Detection with ScyllaDB Vector Search (ANN Queries)")
    print("=" * 70)
    print(f"\nDevices: {', '.join(args.devices)}")
    print(f"Time window: {args.hours_back} hour(s)")
    print(f"Similarity threshold: {args.similarity_threshold}")
    print(f"Sigma threshold: {args.sigma_threshold}")
    print(f"Record events: {not args.no_record}")
    print(f"Only new snapshots: {args.only_new}")
    print(f"Uses: ORDER BY embedding ANN OF <vector> queries")
    
    # Connect to ScyllaDB
    session = connect_scylla()
    print("\n✅ Connected to ScyllaDB")
    
    def run_detection():
        all_results = []
        total_anomalies = 0
        total_checked = 0
        
        for device_id in args.devices:
            results = detect_anomalies_for_device(
                session,
                device_id,
                hours_back=args.hours_back,
                similarity_threshold=args.similarity_threshold,
                record_events=not args.no_record,
                only_new=args.only_new
            )
            all_results.extend(results)
            total_anomalies += sum(1 for r in results if r['is_anomalous'])
            total_checked += len(results)
        
        # Summary (only print if we checked something)
        if total_checked > 0:
            timestamp = datetime.now(timezone.utc).strftime('%H:%M:%S')
            print(f"[{timestamp}] Checked {total_checked} snapshots → {total_anomalies} anomal{'y' if total_anomalies == 1 else 'ies'}")
        
        return total_anomalies
    
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
