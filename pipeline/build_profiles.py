#!/usr/bin/env python3
"""
Build device behavior profiles from historical snapshots.

Creates a "fingerprint" embedding for each device's normal behavior by:
1. Collecting recent snapshots for each device
2. Computing average embedding (centroid) for normal behavior
3. Computing metric statistics (mean, std, min, max)
4. Storing in device_profiles table
"""

import argparse
import os
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import List, Dict
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


def get_device_snapshots(session, device_id: str, days_back: int = 1, 
                         exclude_anomalous: bool = True) -> List[Dict]:
    """Get recent snapshots for a device."""
    # Get date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    
    snapshots = []
    
    # Query each day in the range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        query = """
            SELECT snapshot_time, device_type, location, building_id,
                   metrics, embedding, is_anomalous
            FROM device_state_snapshots
            WHERE device_id = %s AND date = %s
        """
        
        result = session.execute(query, (device_id, date_str))
        
        for row in result:
            # Skip anomalous snapshots if requested
            if exclude_anomalous and row.is_anomalous:
                continue
            
            snapshots.append({
                'snapshot_time': row.snapshot_time,
                'device_type': row.device_type,
                'location': row.location,
                'building_id': row.building_id,
                'metrics': row.metrics,
                'embedding': row.embedding,
                'is_anomalous': row.is_anomalous
            })
        
        current_date += timedelta(days=1)
    
    return snapshots


def compute_profile_embedding(embeddings: List[List[float]]) -> List[float]:
    """
    Compute profile embedding as the centroid (average) of all embeddings.
    
    This represents the "typical" or "normal" behavior for the device.
    """
    if not embeddings:
        return None
    
    # Convert to numpy array for easier computation
    embeddings_array = np.array(embeddings)
    
    # Compute mean across all embeddings
    profile_embedding = np.mean(embeddings_array, axis=0)
    
    return profile_embedding.tolist()


def compute_metric_statistics(snapshots: List[Dict]) -> Dict:
    """
    Compute statistics for each metric across all snapshots.
    
    Returns: {metric_name: {mean, std, min, max}}
    """
    # Collect all metrics across snapshots
    metric_values = {}
    
    for snapshot in snapshots:
        metrics = snapshot['metrics']
        for metric_name, value in metrics.items():
            if metric_name not in metric_values:
                metric_values[metric_name] = []
            metric_values[metric_name].append(value)
    
    # Compute statistics
    statistics = {}
    for metric_name, values in metric_values.items():
        values_array = np.array(values)
        statistics[metric_name] = {
            'mean': float(np.mean(values_array)),
            'std': float(np.std(values_array)),
            'min': float(np.min(values_array)),
            'max': float(np.max(values_array))
        }
    
    return statistics


def save_device_profile(session, device_id: str, profile: Dict):
    """Save device profile to ScyllaDB."""
    # Prepare insert statement
    insert_profile = """
        INSERT INTO device_profiles 
        (device_id, device_type, location, building_id,
         profile_embedding, profile_created_at, profile_updated_at,
         metric_stats, last_seen)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    now = datetime.now(timezone.utc)
    
    session.execute(
        insert_profile,
        (
            device_id,
            profile['device_type'],
            profile['location'],
            profile['building_id'],
            profile['profile_embedding'],
            now,
            now,
            profile['metric_stats'],
            profile['last_seen']
        )
    )
    
    # Update counters
    session.execute(
        "UPDATE device_statistics SET total_snapshots = total_snapshots + %s WHERE device_id = %s",
        (profile['snapshot_count'], device_id)
    )


def build_profile(session, device_id: str, days_back: int = 1,
                  min_snapshots: int = 5) -> Dict:
    """
    Build complete profile for a device.
    
    Args:
        session: Cassandra session
        device_id: Device to profile
        days_back: How many days of history to use
        min_snapshots: Minimum snapshots required to build profile
    
    Returns:
        Profile dict or None if insufficient data
    """
    print(f"\n📊 Building profile for {device_id}...")
    
    # Get snapshots
    snapshots = get_device_snapshots(
        session, 
        device_id, 
        days_back=days_back,
        exclude_anomalous=True
    )
    
    if len(snapshots) < min_snapshots:
        print(f"  ⚠️  Insufficient data: {len(snapshots)} snapshots (need {min_snapshots})")
        return None
    
    print(f"  ✓ Found {len(snapshots)} normal snapshots")
    
    # Extract embeddings
    embeddings = [s['embedding'] for s in snapshots if s['embedding']]
    
    if not embeddings:
        print(f"  ⚠️  No embeddings found")
        return None
    
    # Compute profile embedding (centroid)
    profile_embedding = compute_profile_embedding(embeddings)
    print(f"  ✓ Computed profile embedding (dim={len(profile_embedding)})")
    
    # Compute metric statistics
    metric_stats = compute_metric_statistics(snapshots)
    print(f"  ✓ Computed statistics for {len(metric_stats)} metrics")
    
    # Get most recent snapshot info
    latest_snapshot = max(snapshots, key=lambda s: s['snapshot_time'])
    
    profile = {
        'device_id': device_id,
        'device_type': latest_snapshot['device_type'],
        'location': latest_snapshot['location'],
        'building_id': latest_snapshot['building_id'],
        'profile_embedding': profile_embedding,
        'metric_stats': metric_stats,
        'last_seen': latest_snapshot['snapshot_time'],
        'snapshot_count': len(snapshots)
    }
    
    return profile


def main():
    parser = argparse.ArgumentParser(
        description='Build device behavior profiles from historical data'
    )
    parser.add_argument(
        '--devices',
        nargs='+',
        default=['RTU-001', 'MAU-001', 'CH-001', 'CT-001', 'AC-001'],
        help='Device IDs to profile (default: all known devices)'
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=1,
        help='Days of history to use (default: 1)'
    )
    parser.add_argument(
        '--min-snapshots',
        type=int,
        default=5,
        help='Minimum snapshots required to build profile (default: 5)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔍 Building Device Behavior Profiles")
    print("=" * 60)
    print(f"\nDevices: {', '.join(args.devices)}")
    print(f"History: {args.days_back} day(s)")
    print(f"Min snapshots: {args.min_snapshots}")
    
    # Connect to ScyllaDB
    session = connect_scylla()
    print("\n✅ Connected to ScyllaDB")
    
    # Build profiles for each device
    profiles_created = 0
    profiles_failed = 0
    
    for device_id in args.devices:
        profile = build_profile(
            session,
            device_id,
            days_back=args.days_back,
            min_snapshots=args.min_snapshots
        )
        
        if profile:
            save_device_profile(session, device_id, profile)
            print(f"  ✅ Profile saved to device_profiles table")
            profiles_created += 1
        else:
            profiles_failed += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Profiles created: {profiles_created}")
    print(f"  Failed: {profiles_failed}")
    
    if profiles_created > 0:
        print(f"\n✅ Device profiles ready for anomaly detection!")
        print(f"   Use vector similarity to compare new snapshots against profiles")
    
    session.cluster.shutdown()


if __name__ == '__main__':
    main()
