#!/usr/bin/env python3
"""
Find Similar Device States using ScyllaDB Vector Search.

This script demonstrates ScyllaDB's vector search capabilities by finding
historical device states that are similar to a given snapshot. This is useful for:
- "Has this happened before?"
- Root cause analysis
- Pattern recognition
- Anomaly investigation
"""

import argparse
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import numpy as np

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


def get_snapshot_by_time(session, device_id: str, snapshot_time: datetime):
    """Get a specific snapshot by device and time."""
    date_str = snapshot_time.strftime('%Y-%m-%d')
    
    query = """
        SELECT device_id, snapshot_time, embedding, metrics, is_anomalous, device_type
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s AND snapshot_time = %s
    """
    
    result = session.execute(query, (device_id, date_str, snapshot_time))
    return result.one()


def get_latest_snapshot(session, device_id: str):
    """Get the most recent snapshot for a device."""
    # Try today first
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    query = """
        SELECT device_id, snapshot_time, embedding, metrics, is_anomalous, device_type
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s
        LIMIT 1
    """
    
    result = session.execute(query, (device_id, today))
    row = result.one()
    
    if not row:
        # Try yesterday
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        result = session.execute(query, (device_id, yesterday))
        row = result.one()
    
    return row


def find_similar_snapshots_across_dates(
    session,
    query_embedding: List[float],
    device_id: str,
    days_back: int = 7,
    limit: int = 10
) -> List[Dict]:
    """
    Use Vector Search to find similar snapshots across multiple days.
    
    This demonstrates the power of vector search by finding similar
    device states across the entire history, not just one day.
    """
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    
    all_similar = []
    
    # Query each date in the range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Vector search query - ORDER BY embedding ANN OF
        ann_query = """
            SELECT device_id, snapshot_time, embedding, metrics, is_anomalous
            FROM device_state_snapshots
            WHERE device_id = %s AND date = %s
            ORDER BY embedding ANN OF %s
            LIMIT %s
        """
        
        result = session.execute(
            ann_query,
            (device_id, date_str, query_embedding, limit)
        )
        
        for row in result:
            all_similar.append({
                'device_id': row.device_id,
                'snapshot_time': row.snapshot_time,
                'embedding': row.embedding,
                'metrics': row.metrics,
                'is_anomalous': row.is_anomalous
            })
        
        current_date += timedelta(days=1)
    
    return all_similar


def compute_cosine_similarity(embedding1: list, embedding2: list) -> float:
    """Compute COSINE similarity."""
    vec1 = np.array(embedding1)
    vec2 = np.array(embedding2)
    
    dot_product = np.dot(vec1, vec2)
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return float(dot_product / (norm1 * norm2))


def compare_metrics(metrics1: Dict, metrics2: Dict) -> Dict:
    """Compare two metric snapshots and find differences."""
    differences = {}
    
    all_keys = set(metrics1.keys()) | set(metrics2.keys())
    
    for key in all_keys:
        val1 = metrics1.get(key, 0)
        val2 = metrics2.get(key, 0)
        
        if val1 != val2:
            diff_pct = ((val1 - val2) / val2 * 100) if val2 != 0 else 0
            differences[key] = {
                'current': val1,
                'similar': val2,
                'diff_pct': diff_pct
            }
    
    return differences


def main():
    parser = argparse.ArgumentParser(
        description='Find similar device states using ScyllaDB Vector Search'
    )
    parser.add_argument(
        '--device',
        required=True,
        help='Device ID to analyze'
    )
    parser.add_argument(
        '--time',
        help='Specific snapshot time (ISO format: 2025-11-18T15:30:00Z). Uses latest if not provided.'
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=7,
        help='Days of history to search (default: 7)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=5,
        help='Number of similar snapshots to find (default: 5)'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("🔍 ScyllaDB Vector Search: Find Similar Device States")
    print("=" * 80)
    
    # Connect to ScyllaDB
    session = connect_scylla()
    print("\n✅ Connected to ScyllaDB")
    
    # Get the query snapshot
    if args.time:
        snapshot_time = datetime.fromisoformat(args.time.replace('Z', '+00:00'))
        query_snapshot = get_snapshot_by_time(session, args.device, snapshot_time)
    else:
        query_snapshot = get_latest_snapshot(session, args.device)
    
    if not query_snapshot:
        print(f"\n❌ No snapshot found for {args.device}")
        session.cluster.shutdown()
        return
    
    print(f"\n📊 Query Snapshot:")
    print(f"  Device: {query_snapshot.device_id} ({query_snapshot.device_type})")
    print(f"  Time: {query_snapshot.snapshot_time}")
    print(f"  Anomalous: {query_snapshot.is_anomalous}")
    print(f"  Metrics: {len(query_snapshot.metrics)} values")
    
    # Show some key metrics
    print(f"\n  Key metrics:")
    for i, (key, value) in enumerate(sorted(query_snapshot.metrics.items())[:5]):
        print(f"    {key}: {value:.2f}")
    if len(query_snapshot.metrics) > 5:
        print(f"    ... and {len(query_snapshot.metrics) - 5} more")
    
    print(f"\n🔎 Searching for similar states (using ANN OF query)...")
    print(f"   Search window: {args.days_back} days")
    print(f"   Results: top {args.limit}")
    
    # Find similar snapshots using vector search
    similar_snapshots = find_similar_snapshots_across_dates(
        session,
        query_snapshot.embedding,
        args.device,
        days_back=args.days_back,
        limit=args.limit
    )
    
    if not similar_snapshots:
        print("\n  No similar snapshots found")
        session.cluster.shutdown()
        return
    
    # Compute similarities and sort
    for snap in similar_snapshots:
        snap['similarity'] = compute_cosine_similarity(
            query_snapshot.embedding,
            snap['embedding']
        )
    
    # Remove exact match (same timestamp) and sort by similarity
    similar_snapshots = [s for s in similar_snapshots 
                        if s['snapshot_time'] != query_snapshot.snapshot_time]
    similar_snapshots.sort(key=lambda x: x['similarity'], reverse=True)
    
    # Display top results
    print(f"\n📈 Top {args.limit} Similar States (using Vector Index):")
    print("=" * 80)
    
    for i, snap in enumerate(similar_snapshots[:args.limit], 1):
        similarity_pct = snap['similarity'] * 100
        
        print(f"\n{i}. Snapshot from {snap['snapshot_time']}")
        print(f"   Similarity: {similarity_pct:.2f}% (COSINE)")
        print(f"   Anomalous: {snap['is_anomalous']}")
        
        # Compare metrics
        differences = compare_metrics(query_snapshot.metrics, snap['metrics'])
        
        if differences:
            print(f"   Metric differences: {len(differences)}")
            # Show top 3 differences
            sorted_diffs = sorted(
                differences.items(),
                key=lambda x: abs(x[1]['diff_pct']),
                reverse=True
            )
            for key, diff in sorted_diffs[:3]:
                print(f"     {key}: {diff['current']:.2f} vs {diff['similar']:.2f} "
                      f"({diff['diff_pct']:+.1f}%)")
        else:
            print(f"   Metrics: Nearly identical")
    
    # Analysis
    print("\n" + "=" * 80)
    print("📊 Analysis:")
    
    # Count anomalies in similar snapshots
    similar_anomalies = sum(1 for s in similar_snapshots[:args.limit] if s['is_anomalous'])
    avg_similarity = np.mean([s['similarity'] for s in similar_snapshots[:args.limit]])
    
    print(f"  Average similarity: {avg_similarity*100:.2f}%")
    print(f"  Similar anomalous states: {similar_anomalies}/{args.limit}")
    
    if query_snapshot.is_anomalous:
        if similar_anomalies > 0:
            print(f"\n  💡 This anomaly has occurred {similar_anomalies} time(s) before!")
            print(f"     → Check maintenance logs for these dates")
        else:
            print(f"\n  ⚠️  This is a NEW type of anomaly!")
            print(f"     → Investigate root cause")
    else:
        if similar_anomalies > 0:
            print(f"\n  ⚠️  Current state is normal, but similar to {similar_anomalies} anomalies")
            print(f"     → Monitor closely, may be early warning")
        else:
            print(f"\n  ✅ Current state is normal and similar to other normal operations")
    
    print("\n" + "=" * 80)
    print("Vector Search Query Used:")
    print("  SELECT * FROM device_state_snapshots")
    print("  WHERE device_id = ? AND date = ?")
    print("  ORDER BY embedding ANN OF <query_vector>")
    print("  LIMIT ?")
    print("\n  (Uses device_state_embedding_idx COSINE index)")
    print("=" * 80)
    
    session.cluster.shutdown()


if __name__ == '__main__':
    main()
