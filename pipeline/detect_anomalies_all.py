#!/usr/bin/env python3
"""
Unified Anomaly Detection - All Three Approaches.

Runs all three detection methods and compares results:
1. Rules-Based (hard limits)
2. Profile Similarity (COSINE)
3. Vector Search (ANN queries)
"""

import argparse
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

# Import the three detection modules
from detect_anomalies_rules import (
    connect_scylla,
    analyze_snapshot_rules
)
from detect_anomalies import (
    get_device_profile,
    compute_cosine_similarity,
    check_metric_outliers
)
from detect_anomalies_vector_search import (
    get_recent_snapshots,  # This one includes embeddings
    find_similar_snapshots_vector_search
)

load_dotenv()


def get_snapshots_with_all_fields(session, device_id: str, hours_back: int = 1):
    """Get recent snapshots with all fields needed for all detection methods."""
    from datetime import datetime, timezone, timedelta
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)
    
    snapshots = []
    
    current_date = end_time.strftime('%Y-%m-%d')
    query = """
        SELECT device_id, device_type, snapshot_time, metrics, embedding, is_anomalous
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s
        AND snapshot_time >= %s
    """
    
    result = session.execute(query, (device_id, current_date, start_time))
    snapshots.extend(result)
    
    if start_time.date() < end_time.date():
        prev_date = start_time.strftime('%Y-%m-%d')
        result = session.execute(query, (device_id, prev_date, start_time))
        snapshots.extend(result)
    
    return snapshots


def analyze_with_all_methods(session, device_id: str, hours_back: int = 1):
    """
    Analyze a device using all three detection methods.
    """
    print(f"\n{'='*80}")
    print(f"🔍 Multi-Method Anomaly Detection: {device_id}")
    print(f"{'='*80}")
    
    # Get snapshots (with all fields)
    snapshots = get_snapshots_with_all_fields(session, device_id, hours_back)
    
    if not snapshots:
        print(f"  ℹ️  No recent snapshots found")
        return
    
    device_type = snapshots[0].device_type
    print(f"\nDevice: {device_id} ({device_type})")
    print(f"Snapshots: {len(snapshots)}")
    print(f"Time window: {hours_back} hour(s)\n")
    
    # Get profile for methods 2 & 3
    profile = get_device_profile(session, device_id)
    
    if not profile:
        print("⚠️  No profile found - only rules-based detection available")
        print("   Run: python pipeline/build_profiles.py\n")
    
    # Analyze each snapshot with all methods
    results = []
    
    for i, snapshot in enumerate(snapshots[:5]):  # Show first 5
        print(f"\n{'─'*80}")
        print(f"Snapshot #{i+1}: {snapshot.snapshot_time}")
        print(f"{'─'*80}")
        
        result = {
            'snapshot_time': snapshot.snapshot_time,
            'methods': {}
        }
        
        # Method 1: Rules-Based
        print("\n1️⃣  RULES-BASED (Hard Limits)")
        print("   " + "─"*70)
        
        rules_result = analyze_snapshot_rules(
            device_id,
            device_type,
            snapshot,
            record_events=False
        )
        
        result['methods']['rules'] = rules_result
        
        if rules_result['is_anomalous']:
            print(f"   🚨 ANOMALOUS")
            for v in rules_result['threshold_violations'][:2]:
                print(f"      [{v['severity']}] {v['message']}")
            for v in rules_result['relationship_violations'][:2]:
                print(f"      [{v['severity']}] {v['message']}")
        else:
            print(f"   ✅ NORMAL - All thresholds pass")
        
        # Method 2: Profile Similarity
        if profile:
            print("\n2️⃣  PROFILE SIMILARITY (COSINE)")
            print("   " + "─"*70)
            
            similarity = compute_cosine_similarity(
                snapshot.embedding,
                profile['profile_embedding']
            )
            
            outliers = check_metric_outliers(
                snapshot.metrics,
                profile['metric_stats'],
                sigma_threshold=3.0
            )
            
            is_anomalous = similarity < 0.85 or len(outliers) > 0
            
            result['methods']['profile'] = {
                'similarity': similarity,
                'outliers': outliers,
                'is_anomalous': is_anomalous
            }
            
            if is_anomalous:
                print(f"   🚨 ANOMALOUS")
                if similarity < 0.85:
                    print(f"      Similarity: {similarity:.4f} < 0.85 threshold")
                if outliers:
                    print(f"      Outliers: {', '.join(list(outliers.keys())[:3])}")
            else:
                print(f"   ✅ NORMAL - Similarity: {similarity:.4f}")
        
        # Method 3: Vector Search
        if profile:
            print("\n3️⃣  VECTOR SEARCH (ANN Queries)")
            print("   " + "─"*70)
            
            date_str = snapshot.snapshot_time.strftime('%Y-%m-%d')
            
            similar_snapshots = find_similar_snapshots_vector_search(
                session,
                snapshot.embedding,
                device_id,
                date_str,
                limit=10
            )
            
            similar_normal_count = sum(1 for s in similar_snapshots 
                                      if not s['is_anomalous'])
            
            is_anomalous = similar_normal_count < 3
            
            result['methods']['vector_search'] = {
                'similar_total': len(similar_snapshots),
                'similar_normal': similar_normal_count,
                'is_anomalous': is_anomalous
            }
            
            if is_anomalous:
                print(f"   🚨 ANOMALOUS - Novel behavior")
                print(f"      Similar snapshots found: {len(similar_snapshots)}")
                print(f"      Normal: {similar_normal_count}, Anomalous: {len(similar_snapshots) - similar_normal_count}")
                if similar_normal_count == 0:
                    print(f"      💡 No similar normal behavior in history!")
            else:
                print(f"   ✅ NORMAL - Seen this before")
                print(f"      Similar normal snapshots: {similar_normal_count}/10")
        
        results.append(result)
    
    # Summary comparison
    print(f"\n{'='*80}")
    print(f"📊 DETECTION METHOD COMPARISON")
    print(f"{'='*80}\n")
    
    if profile:
        print(f"{'Method':<25} {'Anomalies':<15} {'Detection Rate'}")
        print(f"{'-'*25} {'-'*15} {'-'*15}")
        
        rules_count = sum(1 for r in results if r['methods']['rules']['is_anomalous'])
        profile_count = sum(1 for r in results if 'profile' in r['methods'] and r['methods']['profile']['is_anomalous'])
        vector_count = sum(1 for r in results if 'vector_search' in r['methods'] and r['methods']['vector_search']['is_anomalous'])
        
        total = len(results)
        
        print(f"{'1. Rules-Based':<25} {rules_count:<15} {rules_count/total*100:.1f}%")
        print(f"{'2. Profile Similarity':<25} {profile_count:<15} {profile_count/total*100:.1f}%")
        print(f"{'3. Vector Search':<25} {vector_count:<15} {vector_count/total*100:.1f}%")
        
        # Consensus
        consensus = 0
        for result in results:
            votes = []
            votes.append(result['methods']['rules']['is_anomalous'])
            if 'profile' in result['methods']:
                votes.append(result['methods']['profile']['is_anomalous'])
            if 'vector_search' in result['methods']:
                votes.append(result['methods']['vector_search']['is_anomalous'])
            
            if sum(votes) >= 2:  # 2+ methods agree it's anomalous
                consensus += 1
        
        print(f"\n{'Consensus (2+ methods)':<25} {consensus:<15} {consensus/total*100:.1f}%")
        
        # Analysis
        print(f"\n📈 Analysis:")
        print(f"   • Rules-based: Fast, deterministic, catches threshold violations")
        print(f"   • Profile similarity: Detects deviation from typical behavior")
        print(f"   • Vector search: Finds novel patterns, historical context")
        
        if rules_count > profile_count + vector_count:
            print(f"\n   💡 Rules catching more anomalies → Hard limit violations")
        elif vector_count > rules_count:
            print(f"\n   💡 Vector search catching more → Novel behavior patterns")
        
        if consensus < min(rules_count, profile_count, vector_count):
            print(f"\n   ⚠️  Methods disagree on {total - consensus} snapshot(s)")
            print(f"       → Consider tuning thresholds or investigating false positives")


def main():
    parser = argparse.ArgumentParser(
        description='Run all three anomaly detection methods and compare results'
    )
    parser.add_argument(
        '--device',
        required=True,
        help='Device ID to analyze'
    )
    parser.add_argument(
        '--hours-back',
        type=int,
        default=1,
        help='Hours of recent snapshots to analyze (default: 1)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("🔍 Multi-Method Anomaly Detection Framework")
    print("="*80)
    print("\nComparing three approaches:")
    print("  1. Rules-Based       → Hard limits + relationships")
    print("  2. Profile Similarity → COSINE distance to baseline")
    print("  3. Vector Search     → ANN queries for historical context")
    
    # Connect to ScyllaDB
    session = connect_scylla()
    print("\n✅ Connected to ScyllaDB\n")
    
    # Run analysis
    analyze_with_all_methods(
        session,
        args.device,
        hours_back=args.hours_back
    )
    
    session.cluster.shutdown()
    
    print("\n" + "="*80)
    print("✅ Analysis Complete")
    print("="*80)


if __name__ == '__main__':
    main()
