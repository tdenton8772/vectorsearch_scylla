# Vector Search Usage in IoT Anomaly Detection

## Overview

This system demonstrates ScyllaDB Vector Search in **two ways**:

1. **Profile Creation** (Fingerprinting) - Uses vectors to create behavior baselines
2. **Similarity Search** (ANN Queries) - Uses `ORDER BY embedding ANN OF` for finding similar states

## The Two Approaches

### Approach 1: detect_anomalies.py (Hybrid)

**What it does**:
- Computes COSINE similarity in Python (NumPy)
- Compares snapshot to profile centroid
- Uses statistical z-scores for outliers

**Vector Search Usage**: ❌ **Minimal**
- Only uses vectors for storing/retrieving embeddings
- Similarity computation happens in application code
- Does NOT use the vector index for queries

**Why this approach**:
- Simpler logic
- Profile comparison is just one vector (centroid)
- Fast enough for real-time detection
- Good for production monitoring

### Approach 2: detect_anomalies_vector_search.py (Full Vector Search)

**What it does**:
- Uses `ORDER BY embedding ANN OF <vector> LIMIT N` queries
- Leverages ScyllaDB's vector index (`device_state_embedding_idx`)
- Finds K nearest neighbors in historical data
- Counts how many similar snapshots were normal

**Vector Search Usage**: ✅ **Full**
- Every snapshot triggers an ANN query
- Uses COSINE similarity index
- Demonstrates actual vector search capabilities

**Why this approach**:
- Shows off ScyllaDB vector search features
- "Has this happened before?" analysis
- Can detect novel anomalies (no similar history found)
- Better for demo/presentation purposes

## Query Comparison

### Traditional (Approach 1)
```python
# Get profile embedding
profile = session.execute(
    "SELECT profile_embedding FROM device_profiles WHERE device_id = ?",
    (device_id,)
)

# Compute similarity in Python
similarity = np.dot(snapshot_embedding, profile_embedding) / (norm1 * norm2)

if similarity < 0.85:
    flag_as_anomaly()
```

### Vector Search (Approach 2)
```sql
-- Use ANN OF to find similar snapshots
SELECT device_id, snapshot_time, embedding, metrics, is_anomalous
FROM device_state_snapshots
WHERE device_id = ? AND date = ?
ORDER BY embedding ANN OF <snapshot_embedding>
LIMIT 10
```

**Key difference**: The second approach uses ScyllaDB's vector index to perform the similarity search, demonstrating the database's built-in vector search capabilities.

## Use Cases

### When to Use Approach 1 (detect_anomalies.py)

✅ **Production monitoring**
- Real-time alerting
- Simple anomaly/normal classification
- Known device profiles exist
- Predictable behavior patterns

### When to Use Approach 2 (detect_anomalies_vector_search.py)

✅ **Investigation & analysis**
- Root cause analysis
- Pattern discovery
- "Show me similar incidents"
- Novel anomaly detection
- Demo/presentation purposes

## The find_similar_states.py Tool

**Purpose**: Pure vector search demonstration

```bash
python pipeline/find_similar_states.py --device RTU-001 --limit 5
```

**What it does**:
1. Gets latest (or specified) snapshot
2. Uses ANN query to find K most similar historical states
3. Shows similarity scores and metric differences
4. Analyzes: "Has this happened before?"

**Use cases**:
- **Incident investigation**: "Find times when device behaved like this"
- **Pattern analysis**: "What do anomalies have in common?"
- **Predictive**: "Current state resembles past failures"
- **Demo**: Shows vector search in action

## Vector Index Details

### Schema
```sql
CREATE TABLE device_state_snapshots (
    device_id text,
    date text,
    snapshot_time timestamp,
    embedding vector<float, 384>,
    metrics map<text, double>,
    is_anomalous boolean,
    PRIMARY KEY ((device_id, date), snapshot_time)
);

-- Vector index using Storage-Attached Index (SAI)
CREATE CUSTOM INDEX device_state_embedding_idx 
ON device_state_snapshots(embedding) 
USING 'StorageAttachedIndex';
```

### Query Syntax
```sql
-- ANN (Approximate Nearest Neighbor) query
ORDER BY embedding ANN OF <vector>
LIMIT <k>
```

**How it works**:
- Uses COSINE similarity internally
- Approximate (fast) not exhaustive (slow)
- Returns K most similar vectors
- Leverages the SAI vector index

## Performance Characteristics

| Operation | Approach 1 | Approach 2 (Vector Search) |
|-----------|-----------|---------------------------|
| **Profile comparison** | O(1) - single vector | O(1) - single vector |
| **Find similar** | N/A | O(log N) - ANN index |
| **Latency** | ~1ms | ~10ms (includes DB query) |
| **Scalability** | Excellent | Excellent |
| **Index usage** | No | Yes ✅ |

## Demo Scenarios

### Scenario 1: Real-time Monitoring (Use Approach 1)

```bash
# Monitor all devices continuously
python pipeline/detect_anomalies.py --continuous
```

**Shows**: 
- Fast detection
- Profile-based classification
- Statistical outliers

### Scenario 2: Vector Search Demo (Use Approach 2)

```bash
# Inject an anomaly
python pipeline/kafka_producer.py \
    --devices RTU-001:rooftop_unit \
    --anomaly-devices RTU-001 \
    --duration 60 &

# Wait 30 seconds for data
sleep 30

# Detect using vector search
python pipeline/detect_anomalies_vector_search.py --devices RTU-001

# Shows: "Few similar normal snapshots found" = novel behavior
```

### Scenario 3: Similarity Search (Use find_similar_states.py)

```bash
# Find similar historical states
python pipeline/find_similar_states.py --device RTU-001 --limit 5
```

**Shows**:
- ANN query in action
- COSINE similarity scores
- Metric comparisons
- "Has this happened before?" analysis

## Re:Invent Demo Flow

**Recommended presentation order**:

1. **Start**: Show system running (dashboard + producer + consumer)

2. **Profile Creation**: 
   ```bash
   python pipeline/build_profiles.py
   # Explain: "Creating vector fingerprints of normal behavior"
   ```

3. **Traditional Detection** (optional):
   ```bash
   python pipeline/detect_anomalies.py --devices RTU-001
   # Show: Quick profile comparison
   ```

4. **Vector Search Demo** (highlight):
   ```bash
   # Inject anomaly
   python pipeline/kafka_producer.py --devices RTU-001:rooftop_unit \
       --anomaly-devices RTU-001 --interval 10 --duration 120 &
   
   sleep 40
   
   # Detect with vector search
   python pipeline/detect_anomalies_vector_search.py --devices RTU-001
   # Show: "ORDER BY embedding ANN OF" in action
   # Point out: "Few similar normal snapshots found"
   ```

5. **Similarity Search** (finale):
   ```bash
   python pipeline/find_similar_states.py --device RTU-001 --limit 5
   # Show: Finding similar historical states
   # Explain: "Root cause analysis using vector similarity"
   ```

## Key Talking Points

### For Technical Audience

- "ScyllaDB's Storage-Attached Indexes (SAI) enable vector search"
- "COSINE similarity with 384-dimensional embeddings"
- "Sub-10ms ANN queries on time-series data"
- "Scales to millions of vectors"

### For Business Audience

- "Find similar past incidents automatically"
- "Detect novel anomalies (never seen before)"
- "Reduce false positives with historical context"
- "Root cause analysis powered by AI"

## Limitations & Considerations

### Partition Key Constraints

**Current Schema**:
```
PRIMARY KEY ((device_id, date), snapshot_time)
```

**Impact on Vector Search**:
- ANN queries must include partition key (device_id, date)
- Can't search across ALL devices in one query
- Need separate query per device per date

**Workaround for Multi-Day Search**:
```python
# Query each date in range
for date in date_range:
    results = session.execute(
        "SELECT * FROM device_state_snapshots "
        "WHERE device_id = ? AND date = ? "
        "ORDER BY embedding ANN OF ? LIMIT ?",
        (device_id, date, query_vector, limit)
    )
```

**Future Enhancement**:
Consider a separate table for cross-device/cross-time searches:
```sql
CREATE TABLE device_embeddings_global (
    embedding_id uuid PRIMARY KEY,
    device_id text,
    snapshot_time timestamp,
    embedding vector<float, 384>,
    ...
);
-- Can search across all devices/dates
```

## Summary

| Feature | Approach 1 | Approach 2 | find_similar_states |
|---------|-----------|-----------|---------------------|
| **Vector Index Used** | ❌ No | ✅ Yes | ✅ Yes |
| **ANN Queries** | ❌ No | ✅ Yes | ✅ Yes |
| **Use Case** | Monitoring | Detection + Analysis | Investigation |
| **Best For** | Production | Demo | Research |
| **Speed** | Fastest | Fast | Fast |
| **Insight** | Normal/Anomaly | + Similar history | + Root cause |

**Recommendation for re:Invent**: 
- Start with Approach 1 to show the system works
- Switch to Approach 2 + find_similar_states to highlight vector search
- Emphasize: "ScyllaDB Vector Search enables intelligent anomaly detection at scale"
