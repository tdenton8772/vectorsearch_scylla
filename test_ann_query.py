#!/usr/bin/env python3
"""
Test ScyllaDB ANN (Approximate Nearest Neighbor) queries with real embedding data.

This script gets a real embedding from the database and runs an ANN query
to find similar snapshots.
"""

from pipeline.detect_anomalies_vector_search import connect_scylla
from datetime import datetime, timezone

def test_ann_query():
    session = connect_scylla()
    
    # Step 1: Get a recent snapshot with embedding
    print("=" * 70)
    print("Step 1: Getting a recent snapshot with embedding")
    print("=" * 70)
    
    query = '''
        SELECT device_id, snapshot_time, embedding
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s
        ORDER BY snapshot_time DESC
        LIMIT 1
    '''
    
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    row = session.execute(query, ('RTU-001', today)).one()
    
    print(f"Device: {row.device_id}")
    print(f"Snapshot time: {row.snapshot_time}")
    print(f"Embedding dimensions: {len(row.embedding)}")
    print(f"First 5 values: {row.embedding[:5]}")
    
    test_embedding = row.embedding
    
    # Step 2: Run ANN query
    print("\n" + "=" * 70)
    print("Step 2: Running ANN query to find similar snapshots")
    print("=" * 70)
    
    ann_query = '''
        SELECT device_id, snapshot_time, embedding
        FROM device_state_snapshots
        ORDER BY embedding ANN OF %s
        LIMIT 10
    '''
    
    print(f"\nQuery: {ann_query}")
    print(f"With embedding vector of {len(test_embedding)} dimensions")
    
    try:
        results = list(session.execute(ann_query, (test_embedding,)))
        print(f"\n✅ SUCCESS: ANN query returned {len(results)} results")
        
        if results:
            print("\nResults:")
            for i, result in enumerate(results, 1):
                print(f"  {i}. Device: {result.device_id}, Time: {result.snapshot_time}")
                
                # Compute similarity
                import numpy as np
                vec1 = np.array(test_embedding)
                vec2 = np.array(result.embedding)
                similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
                print(f"     Cosine similarity: {similarity:.4f}")
        else:
            print("\n⚠️  Query succeeded but returned 0 results")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
    
    # Step 3: Try with different LIMIT values
    print("\n" + "=" * 70)
    print("Step 3: Testing different LIMIT values")
    print("=" * 70)
    
    for limit in [1, 5, 10, 50, 100]:
        try:
            ann_query_limit = f'''
                SELECT device_id, snapshot_time
                FROM device_state_snapshots
                ORDER BY embedding ANN OF %s
                LIMIT {limit}
            '''
            results = list(session.execute(ann_query_limit, (test_embedding,)))
            print(f"LIMIT {limit:3d}: {len(results)} results")
        except Exception as e:
            print(f"LIMIT {limit:3d}: ERROR - {e}")
    
    session.cluster.shutdown()

if __name__ == '__main__':
    test_ann_query()
