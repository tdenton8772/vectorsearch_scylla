# Three Paths to Anomaly Detection

## Overview

This system implements **three complementary anomaly detection approaches**, each with different strengths:

1. **Rules-Based** - Traditional threshold monitoring
2. **Profile Similarity** - ML-based deviation detection  
3. **Vector Search** - Novel pattern discovery

## The Three Approaches

### 1. Rules-Based Detection 🔧

**File**: `detect_anomalies_rules.py`

**How it works**:
- Hard-coded min/max thresholds per metric per device type
- Cross-metric relationship rules (e.g., "if compressor on, power must be > 5kW")
- Severity levels (CRITICAL vs WARNING)

**Example Rules**:
```python
DEVICE_RULES = {
    'rooftop_unit': {
        'supply_air_temp': {'min': 50, 'max': 90, 'critical': True},
        'power_consumption': {'min': 0, 'max': 50, 'critical': True},
    }
}

RELATIONSHIP_RULES = {
    'rooftop_unit': [
        {
            'condition': lambda m: m['compressor_status'] == 1 and m['power_consumption'] < 5,
            'message': 'Compressor on but power too low'
        }
    ]
}
```

**Pros**:
- ✅ Fastest (<1ms per snapshot)
- ✅ Deterministic and explainable
- ✅ No training data required
- ✅ Catches safety violations immediately
- ✅ Easy for operators to understand

**Cons**:
- ❌ Requires domain expertise to set thresholds
- ❌ Brittle (doesn't adapt to changing conditions)
- ❌ Can't detect subtle multi-metric patterns
- ❌ High maintenance (rules need updating)

**Best for**:
- Safety-critical limits
- Regulatory compliance
- Known failure modes
- Real-time alerting

### 2. Profile Similarity Detection 🎯

**File**: `detect_anomalies.py`

**How it works**:
- Create "fingerprint" = centroid of normal behavior embeddings
- Compare new snapshots to fingerprint using COSINE similarity
- Statistical outlier detection (z-scores)

**Algorithm**:
```python
# 1. Build profile (one-time)
profile_embedding = mean(all_normal_embeddings)  # 384-dim vector

# 2. For each new snapshot
similarity = cosine_similarity(snapshot_embedding, profile_embedding)

if similarity < 0.85:
    flag_as_anomaly()
```

**Pros**:
- ✅ Fast (~1-2ms per snapshot)
- ✅ Captures holistic device behavior
- ✅ Adapts to normal variations automatically
- ✅ No manual threshold tuning
- ✅ Production-ready

**Cons**:
- ❌ Requires training period (5+ snapshots)
- ❌ Doesn't use vector search capabilities
- ❌ Single profile may not capture all normal modes
- ❌ Can miss gradual drift

**Best for**:
- 24/7 monitoring
- Production deployments
- When you have baseline data
- Quick deviation detection

### 3. Vector Search Detection 🔎

**File**: `detect_anomalies_vector_search.py`

**How it works**:
- Use ScyllaDB ANN queries to find K most similar historical states
- Count how many were normal vs anomalous
- Logic: Few similar normal states = novel anomaly

**Algorithm**:
```sql
-- For each snapshot, run ANN query
SELECT * FROM device_state_snapshots
WHERE device_id = ? AND date = ?
ORDER BY embedding ANN OF <snapshot_embedding>
LIMIT 10
```

```python
# Count normal snapshots
similar_normal = sum(1 for s in results if not s['is_anomalous'])

if similar_normal < 3:
    flag_as_novel_anomaly()
```

**Pros**:
- ✅ Detects truly novel failures
- ✅ Uses ScyllaDB vector index (demo-worthy!)
- ✅ Provides historical context ("seen this 5 times before")
- ✅ Can find similar past incidents
- ✅ Excellent for investigation

**Cons**:
- ❌ Slower (~10ms per snapshot)
- ❌ Requires significant historical data
- ❌ Partition key constraints (must query by device+date)
- ❌ More complex logic

**Best for**:
- Re:Invent demos
- Root cause analysis
- Novel anomaly detection
- "Has this happened before?" queries

## Comparison Matrix

| Feature | Rules-Based | Profile Similarity | Vector Search |
|---------|-------------|-------------------|---------------|
| **Speed** | Fastest (<1ms) | Fast (~2ms) | Good (~10ms) |
| **Accuracy** | Depends on rules | High | Very High |
| **Explainability** | Perfect | Good | Excellent |
| **Training Required** | No | Yes (5+ snapshots) | Yes (many snapshots) |
| **Vector Index Used** | ❌ No | ❌ No | ✅ Yes |
| **Detects Novel Patterns** | ❌ No | ⚠️  Partial | ✅ Yes |
| **Domain Knowledge** | High | Low | Low |
| **Maintenance** | High | Low | Low |
| **False Positives** | Medium-High | Low-Medium | Low |
| **False Negatives** | Medium | Low-Medium | Very Low |

## When to Use Each

### Rules-Based ✅

**Use when**:
- You have safety limits that must never be exceeded
- Regulatory compliance requirements
- You know exactly what "bad" looks like
- Speed is critical (milliseconds matter)
- You need deterministic, explainable alerts

**Example**: "Chilled water supply temp must never exceed 55°F (safety)"

### Profile Similarity ✅

**Use when**:
- You need 24/7 production monitoring
- You have historical normal data
- You want to catch deviations from typical behavior
- Speed matters but not critical
- You want low maintenance

**Example**: "Device behaving differently than its typical pattern"

### Vector Search ✅

**Use when**:
- Demonstrating ScyllaDB capabilities
- Investigating specific incidents
- You want to find similar past failures
- Novel anomaly detection is priority
- Root cause analysis

**Example**: "Has this exact failure mode occurred before?"

## Combined Approach (Recommended)

**Use all three together** for maximum coverage:

```
┌─────────────────────────────────────────┐
│ Incoming Device Snapshot                │
└──────────────┬──────────────────────────┘
               │
      ┌────────┼────────┐
      │        │        │
      ▼        ▼        ▼
  ┌──────┐ ┌──────┐ ┌──────┐
  │Rules │ │Profile│ │Vector│
  │Based │ │ Sim  │ │Search│
  └───┬──┘ └───┬──┘ └───┬──┘
      │        │        │
      └────────┼────────┘
               │
         ┌─────▼─────┐
         │ Consensus │
         │ Decision  │
         └───────────┘
```

**Decision Logic**:
- **3/3 agree**: High confidence anomaly
- **2/3 agree**: Probable anomaly (investigate)
- **1/3 (Rules only)**: Hard limit violation (critical)
- **1/3 (Vector only)**: Novel pattern (research)
- **0/3 agree**: Normal operation

## Demo Script Commands

### Run Individual Methods

```bash
# 1. Rules-Based
python pipeline/detect_anomalies_rules.py --devices RTU-001

# 2. Profile Similarity
python pipeline/detect_anomalies.py --devices RTU-001

# 3. Vector Search
python pipeline/detect_anomalies_vector_search.py --devices RTU-001
```

### Run All Three & Compare

```bash
# Unified comparison
python pipeline/detect_anomalies_all.py --device RTU-001

# Output shows:
#   Method                Anomalies       Detection Rate
#   -------------------- --------------- ---------------
#   1. Rules-Based       2               40.0%
#   2. Profile Similarity 1               20.0%
#   3. Vector Search     3               60.0%
#   
#   Consensus (2+ methods) 1             20.0%
```

## Re:Invent Demo Flow

**1. Start with Rules** (Traditional Approach)
```bash
python pipeline/detect_anomalies_rules.py --devices RTU-001
```
*"This is how most IoT monitoring works today - hard limits"*

**2. Show Profile Similarity** (ML Improvement)
```bash
python pipeline/detect_anomalies.py --devices RTU-001
```
*"ML enables detecting deviations from normal patterns"*

**3. Highlight Vector Search** (ScyllaDB Differentiator)
```bash
python pipeline/detect_anomalies_vector_search.py --devices RTU-001
```
*"ScyllaDB Vector Search finds novel failures AND similar past incidents"*

**4. Compare All Three** (Power of Combined Approach)
```bash
python pipeline/detect_anomalies_all.py --device RTU-001
```
*"Each method catches different types of anomalies - together they're comprehensive"*

## Key Talking Points

**For Technical Audience**:
- "Rules catch known failures, Profile catches deviations, Vector Search catches novel patterns"
- "Vector Search uses ScyllaDB's Storage-Attached Indexes for sub-10ms ANN queries"
- "COSINE similarity on 384-dimensional embeddings at database speed"

**For Business Audience**:
- "Three layers of protection: safety rules + learned baselines + historical intelligence"
- "Vector Search answers 'Has this happened before?' automatically"
- "Reduces false positives by considering multiple detection methods"

## Future Enhancements

1. **Weighted Voting**: Different weights for each method based on confidence
2. **Adaptive Thresholds**: Rules that adjust based on profiles
3. **Ensemble Learning**: Combine all three scores into single anomaly score
4. **Feedback Loop**: Use confirmed anomalies to retrain profiles
5. **Cross-Device Search**: "Find other devices with similar failures"

## Summary

| Approach | Speed | Coverage | Demo Value |
|----------|-------|----------|------------|
| **Rules-Based** | ⚡⚡⚡ | Known failures | ⭐⭐ |
| **Profile Similarity** | ⚡⚡ | Typical deviations | ⭐⭐⭐ |
| **Vector Search** | ⚡ | Novel patterns + history | ⭐⭐⭐⭐⭐ |
| **All Three Combined** | ⚡⚡ | Comprehensive | ⭐⭐⭐⭐⭐ |

**Recommendation**: Use all three methods in production for comprehensive coverage, but highlight Vector Search in demos to showcase ScyllaDB's unique capabilities.
