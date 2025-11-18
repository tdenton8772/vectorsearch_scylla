# Anomaly Detection: Profile-Based vs Vector Search

## Quick Comparison

| Feature | Profile-Based | Vector Search |
|---------|--------------|---------------|
| **File** | `detect_anomalies.py` | `detect_anomalies_vector_search.py` |
| **Uses Vector Index** | ❌ No | ✅ Yes |
| **Query Type** | Simple SELECT | ANN (`ORDER BY embedding ANN OF`) |
| **Similarity Computation** | NumPy (in app) | ScyllaDB (in database) |
| **Detection Logic** | Compare to profile centroid | Count similar normal states |
| **Speed** | ~1ms per snapshot | ~10ms per snapshot |
| **Best For** | Production monitoring | Demos, investigations |
| **Detects** | Profile deviations | Novel behaviors |

## Detection Logic Comparison

### Profile-Based (Simple)

```python
# 1. Load profile (one vector - the centroid)
profile_embedding = get_profile(device_id)

# 2. Compute similarity in Python
similarity = cosine_similarity(snapshot_embedding, profile_embedding)

# 3. Decision
if similarity < 0.85:
    return "ANOMALOUS"
else:
    return "NORMAL"
```

**Pros**:
- Very fast (single vector comparison)
- Simple logic
- Good for real-time monitoring

**Cons**:
- Doesn't use vector search capabilities
- Less context (just one profile vector)
- Can't detect "novel" anomalies as well

### Vector Search (Powerful)

```python
# 1. Query similar historical states using vector index
query = """
    SELECT device_id, snapshot_time, embedding, is_anomalous
    FROM device_state_snapshots
    WHERE device_id = ? AND date = ?
    ORDER BY embedding ANN OF ?  ← Vector search!
    LIMIT 10
"""
similar_snapshots = session.execute(query, (device_id, date, snapshot_embedding))

# 2. Count how many were normal
normal_count = sum(1 for s in similar_snapshots if not s['is_anomalous'])

# 3. Decision (inverted logic!)
if normal_count < 3:
    return "ANOMALOUS - Novel behavior!"
else:
    return "NORMAL - Seen this before"
```

**Pros**:
- Uses ScyllaDB vector search (demo-worthy!)
- Historical context (looks at past behavior)
- Detects truly novel anomalies
- Can answer "Has this happened before?"

**Cons**:
- Slightly slower (database query)
- More complex logic
- Requires historical data

## The "Inverted" Logic Explained

**Traditional thinking**: "Find things that are DIFFERENT"
**Vector search thinking**: "Find things that are SIMILAR"

### Why This Works

Vector search finds **similar** states, not different ones. So:

```
If many similar states were NORMAL:
  → Current state is probably NORMAL too
  
If few/no similar states were NORMAL:
  → Current state is probably ANOMALOUS
```

### Real Example

**Scenario**: RTU compressor stuck on

**What happens**:
1. Query: "Find 10 snapshots similar to current state"
2. Results: 10 similar snapshots found
3. Check: All 10 were also marked as anomalous (compressor stuck)
4. Count: 0 normal snapshots found
5. **Decision**: ANOMALOUS (and we've seen this problem 10 times before!)

**Insight**: Vector search not only detected the anomaly, but also found that this exact issue has happened before!

## When to Use Each

### Use Profile-Based (`detect_anomalies.py`)

✅ **Production monitoring**
- 24/7 continuous monitoring
- Real-time alerting
- High-volume scenarios
- When speed matters most

✅ **Simple deployments**
- Single device type
- Predictable behavior patterns
- Limited historical data

### Use Vector Search (`detect_anomalies_vector_search.py`)

✅ **Demonstrations**
- re:Invent presentations
- Showcasing vector search capabilities
- Technical demos
- Sales presentations

✅ **Investigations**
- Root cause analysis
- "Has this happened before?"
- Pattern discovery
- Anomaly research

✅ **Novel anomaly detection**
- Detect truly new failures
- Early warning systems
- Complex environments

## Code Examples

### Example 1: Production Monitoring

```bash
# Start continuous monitoring (fast)
python pipeline/detect_anomalies.py --continuous

# Output every 30 seconds:
# ✅ RTU-001: All snapshots normal (avg similarity: 0.998)
# 🚨 MAU-001: 1 anomaly detected!
#     2025-11-18 16:00:00: Low profile similarity: 0.823 < 0.85
```

### Example 2: Investigation with Vector Search

```bash
# Investigate specific device
python pipeline/detect_anomalies_vector_search.py --devices RTU-001

# Output:
# 🔍 Analyzing RTU-001 (with Vector Search)...
#   ✓ Analyzing 50 snapshot(s) with ANN queries...
#   🚨 5 anomalies detected!
#       2025-11-18 16:00:00: Few similar normal snapshots: 1 < 3
#          (Similar normal: 1, Profile sim: 0.998)
# 
# Insight: This is a NOVEL anomaly - only 1 similar normal state found!
```

### Example 3: Find Similar States

```bash
# "Has this anomaly happened before?"
python pipeline/find_similar_states.py --device RTU-001 --limit 5

# Output:
# 📈 Top 5 Similar States (using Vector Index):
# 
# 1. Snapshot from 2025-11-18 14:30:00
#    Similarity: 99.2% (COSINE)
#    Anomalous: True
#    Metric differences: 2
#      compressor_status: 1.00 vs 1.00 (+0.0%)
#      fan_speed: 810.5 vs 805.2 (+0.7%)
# 
# 💡 This anomaly has occurred 4 time(s) before!
#    → Check maintenance logs for these dates
```

## Performance Comparison

### Profile-Based
```
Per snapshot:
  - Load profile: <1ms
  - Compute similarity: <1ms
  - Total: ~1-2ms

For 100 devices:
  - Check all: ~100-200ms
  - Real-time capable: ✅ Yes
```

### Vector Search
```
Per snapshot:
  - ANN query: ~5-10ms (includes network + DB)
  - Process results: <1ms
  - Total: ~10ms

For 100 devices:
  - Check all: ~1 second
  - Real-time capable: ✅ Yes (with batching)
```

## ScyllaDB Vector Index Usage

### Profile-Based
```sql
-- Just retrieves vectors (no index used)
SELECT profile_embedding 
FROM device_profiles 
WHERE device_id = ?;

-- Similarity computed in Python
```

**Index Used**: ❌ None

### Vector Search
```sql
-- Uses vector index for ANN search
SELECT device_id, snapshot_time, embedding, is_anomalous
FROM device_state_snapshots
WHERE device_id = ? AND date = ?
ORDER BY embedding ANN OF ?  ← Uses device_state_embedding_idx
LIMIT 10;
```

**Index Used**: ✅ `device_state_embedding_idx` (COSINE, Storage-Attached Index)

## Summary

**For re:Invent Demo**:
1. Start with profile-based to show system works ✅
2. Switch to vector search to highlight ScyllaDB capabilities 🎯
3. Use find_similar_states for "wow" factor 🌟

**Key Message**: 
> "ScyllaDB Vector Search enables intelligent anomaly detection by finding similar historical states in milliseconds, allowing us to not just detect anomalies, but understand their context and history."
