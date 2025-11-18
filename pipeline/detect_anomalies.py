#!/usr/bin/env python3
"""
Detect anomalies by comparing device snapshots to behavior profiles.

Uses vector similarity (COSINE) between snapshot embeddings and profile embeddings
to identify when a device is behaving abnormally.
"""

import argparse
import os
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
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
SIMILARITY_THRESHOLD = 0.85  # Below this = anomalous (COSINE similarity, 1.0 = identical)


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


def get_recent_snapshots(session, device_id: str, hours_back: int = 1):
    """Get recent snapshots for a device."""
    # Get date range (might span 2 days)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)
    
    snapshots = []
    
    # Query current day
    current_date = end_time.strftime('%Y-%m-%d')
    query = """
        SELECT snapshot_time, embedding, metrics, is_anomalous
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
    
    return snapshots


def compute_cosine_similarity(embedding1: list, embedding2: list) -> float:
    """
    Compute COSINE similarity between two embeddings.
    
    Returns: Value between -1 and 1 (1 = identical, 0 = orthogonal, -1 = opposite)
    """
    vec1 = np.array(embedding1)
    vec2 = np.array(embedding2)
    
    # Compute cosine similarity
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
    
    Returns: Dict of metric names with outlier info
    """
    outliers = {}
    
    for metric_name, value in snapshot_metrics.items():
        if metric_name not in profile_stats:
            continue
        
        stats = profile_stats[metric_name]
        mean = stats['mean']
        std = stats['std']
        
        # Skip if std is 0 (constant metric)
        if std == 0:
            continue
        
        # Compute z-score
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


def record_anomaly(session, device_id: str, snapshot_time: datetime,
                   similarity_score: float, outlier_metrics: Dict):
    """Record an anomaly event in ScyllaDB."""
    date_str = snapshot_time.strftime('%Y-%m-%d')
    
    # Insert into anomaly_events
    insert_event = """
        INSERT INTO anomaly_events
        (device_id, detected_at, anomaly_score, anomaly_type, metrics_snapshot)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    anomaly_score = 1.0 - similarity_score  # Convert similarity to anomaly score
    
    # Prepare metrics snapshot
    metrics_snapshot = {
        'similarity_score': similarity_score,
        'outlier_count': len(outlier_metrics),
        'outliers': outlier_metrics
    }
    
    session.execute(
        insert_event,
        (device_id, snapshot_time, anomaly_score, 'vector_similarity', metrics_snapshot)
    )
    
    # Update device statistics
    session.execute(
        "UPDATE device_statistics SET anomalies_detected = anomalies_detected + 1 WHERE device_id = %s",
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


def analyze_snapshot(session, device_id: str, snapshot, profile: Dict,
                     similarity_threshold: float = SIMILARITY_THRESHOLD,
                     sigma_threshold: float = 3.0) -> Dict:
    """
    Analyze a snapshot against the device profile.
    
    Returns: Analysis results with anomaly detection
    """
    # Compute vector similarity
    similarity_score = compute_cosine_similarity(
        snapshot.embedding,
        profile['profile_embedding']
    )
    
    # Check metric outliers
    outlier_metrics = check_metric_outliers(
        snapshot.metrics,
        profile['metric_stats'],
        sigma_threshold=sigma_threshold
    )
    
    # Determine if anomalous
    is_anomalous = (
        similarity_score < similarity_threshold or
        len(outlier_metrics) > 0
    )
    
    result = {
        'device_id': device_id,
        'snapshot_time': snapshot.snapshot_time,
        'similarity_score': similarity_score,
        'outlier_metrics': outlier_metrics,
        'is_anomalous': is_anomalous,
        'anomaly_reasons': []
    }
    
    # Add anomaly reasons
    if similarity_score < similarity_threshold:
        result['anomaly_reasons'].append(
            f"Low similarity: {similarity_score:.3f} < {similarity_threshold}"
        )
    
    if outlier_metrics:
        result['anomaly_reasons'].append(
            f"Outlier metrics: {', '.join(outlier_metrics.keys())}"
        )
    
    return result


def detect_anomalies_for_device(session, device_id: str,
                                 hours_back: int = 1,
                                 similarity_threshold: float = SIMILARITY_THRESHOLD,
                                 record_events: bool = True) -> list:
    """
    Detect anomalies for a single device.
    
    Returns: List of analysis results
    """
    print(f"\n🔍 Analyzing {device_id}...")
    
    # Get device profile
    profile = get_device_profile(session, device_id)
    if not profile:
        print(f"  ⚠️  No profile found - run build_profiles.py first")
        return []
    
    print(f"  ✓ Profile loaded (created {profile['profile_created_at']})")
    
    # Get recent snapshots
    snapshots = get_recent_snapshots(session, device_id, hours_back=hours_back)
    
    if not snapshots:
        print(f"  ℹ️  No recent snapshots found")
        return []
    
    print(f"  ✓ Analyzing {len(snapshots)} snapshot(s)...")
    
    # Analyze each snapshot
    results = []
    anomalies_found = 0
    
    for snapshot in snapshots:
        if not snapshot.embedding:
            continue
        
        result = analyze_snapshot(
            session,
            device_id,
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
                    snapshot.snapshot_time,
                    result['similarity_score'],
                    result['outlier_metrics']
                )
    
    # Print summary
    if anomalies_found > 0:
        print(f"  🚨 {anomalies_found} anomal{'y' if anomalies_found == 1 else 'ies'} detected!")
        for result in results:
            if result['is_anomalous']:
                print(f"      {result['snapshot_time']}: {', '.join(result['anomaly_reasons'])}")
    else:
        print(f"  ✅ All snapshots normal (avg similarity: {np.mean([r['similarity_score'] for r in results]):.3f})")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Detect anomalies by comparing snapshots to device profiles'
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
        '--similarity-threshold',
        type=float,
        default=SIMILARITY_THRESHOLD,
        help=f'Similarity threshold for anomaly detection (default: {SIMILARITY_THRESHOLD})'
    )
    parser.add_argument(
        '--sigma-threshold',
        type=float,
        default=3.0,
        help='Standard deviations for metric outlier detection (default: 3.0)'
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
    
    print("=" * 60)
    print("🔍 Anomaly Detection")
    print("=" * 60)
    print(f"\nDevices: {', '.join(args.devices)}")
    print(f"Time window: {args.hours_back} hour(s)")
    print(f"Similarity threshold: {args.similarity_threshold}")
    print(f"Sigma threshold: {args.sigma_threshold}")
    print(f"Record events: {not args.no_record}")
    
    # Connect to ScyllaDB
    session = connect_scylla()
    print("\n✅ Connected to ScyllaDB")
    
    def run_detection():
        all_results = []
        total_anomalies = 0
        
        for device_id in args.devices:
            results = detect_anomalies_for_device(
                session,
                device_id,
                hours_back=args.hours_back,
                similarity_threshold=args.similarity_threshold,
                record_events=not args.no_record
            )
            all_results.extend(results)
            total_anomalies += sum(1 for r in results if r['is_anomalous'])
        
        # Summary
        print("\n" + "=" * 60)
        print(f"Summary: {total_anomalies} anomal{'y' if total_anomalies == 1 else 'ies'} "
              f"out of {len(all_results)} snapshots")
        
        return total_anomalies
    
    if args.continuous:
        import time
        print("\n🔄 Running continuously (Ctrl+C to stop)...")
        try:
            while True:
                run_detection()
                print(f"\n💤 Sleeping 30 seconds...\n")
                time.sleep(30)
        except KeyboardInterrupt:
            print("\n\n✋ Stopped by user")
    else:
        run_detection()
    
    session.cluster.shutdown()


if __name__ == '__main__':
    main()
