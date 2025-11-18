# ScyllaDB Vector Search IoT Anomaly Detection

**Real-time HVAC device monitoring with AI-powered anomaly detection using ScyllaDB Vector Search**

This system demonstrates how ScyllaDB's vector search capabilities enable intelligent anomaly detection for IoT devices by creating "fingerprints" of normal device behavior and comparing new data against these baselines using cosine similarity.

## Project Structure

```
.
├── iot_simulator/          # IoT device simulator for Phase 1
│   ├── iot_simulator.py    # Main simulator with stateful metrics
│   └── test_fleet.sh       # Test script for running device fleet
│
├── scylladb_setup/         # ScyllaDB schema setup
│   ├── create_iot_schema.py  # Create IoT keyspace/tables
│   └── check_setup.py      # Verify schema and connection
│
├── pipeline/               # Data pipeline (Phase 2 & 3)
│   ├── kafka_producer.py   # Device simulators → Kafka
│   ├── kafka_consumer.py   # Kafka → ScyllaDB Buffer → Ollama → ScyllaDB
│   ├── build_profiles.py   # Build device fingerprints
│   ├── detect_anomalies_rules.py  # Method 1: Rules-based detection
│   ├── detect_anomalies.py        # Method 2: Profile similarity
│   ├── detect_anomalies_vector_search.py  # Method 3: Vector search
│   ├── detect_anomalies_all.py    # Compare all three methods
│   ├── find_similar_states.py     # Investigation tool
│   └── test_anomaly_detection.sh  # End-to-end test
│
├── dashboard/              # Dash UI (Phase 4)
│   └── app.py              # Real-time device monitoring
│
├── docs/                   # Documentation
│   ├── device_specifications.md  # HVAC device metrics
│   ├── THREE_DETECTION_PATHS.md  # Complete comparison of all methods
│   ├── DETECTION_COMPARISON.md  # Quick reference guide
│   ├── VECTOR_SEARCH_USAGE.md   # Vector search detailed usage
│   └── SCYLLADB_VECTOR_SEARCH_REFERENCE.md  # Technical reference
│
├── .env                    # Configuration (ScyllaDB credentials)
├── .env.example            # Template
├── config.yaml             # Ollama/embeddings config
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

## Table of Contents

- [System Overview](#system-overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [System Components](#system-components)
- [Anomaly Detection Logic](#anomaly-detection-logic)
- [Data Flow](#data-flow)
- [Operations Guide](#operations-guide)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [References](#references)

---

## System Overview

### What This System Does

1. **Simulates** 5 types of HVAC devices (RTU, MAU, Chiller, Cooling Tower, Air Compressor)
2. **Streams** device metrics through Kafka in real-time
3. **Aggregates** metrics and generates 384-dimensional embeddings via Ollama
4. **Stores** time-series data and embeddings in ScyllaDB Cloud
5. **Creates fingerprints** of normal device behavior using vector centroids
6. **Detects anomalies** by comparing new device states to profiles using COSINE similarity
7. **Visualizes** device status in a real-time Dash dashboard

### Key Technologies

- **ScyllaDB Cloud**: Vector search, time-series storage, and metric aggregation (2025.4.0 RC)
- **Kafka**: Message streaming and event processing
- **Ollama**: Local embedding generation (all-minilm:l6-v2)
- **Dash**: Real-time visualization dashboard
- **Python**: Application logic and simulators

### Demo Use Case

**HVAC Monitoring in a Commercial Building**

Monitor 5 HVAC devices across a building, detecting when devices operate abnormally:
- Compressor stuck on/off
- Temperature sensor failures
- Efficiency degradation
- Multiple correlated metric deviations

The system learns "normal" behavior patterns and flags deviations automatically.

---

## Architecture

### High-Level Data Flow

```
┌─────────────────┐
│ Device Simulators│  (5 HVAC devices)
│ • RTU-001       │  
│ • MAU-001       │  14-15 metrics each
│ • CH-001        │  every 10 seconds
│ • CT-001        │
│ • AC-001        │
└────────┬────────┘
         │ Individual metric messages
         ▼
┌─────────────────┐
│  Kafka Topic    │  iot-metrics (3 partitions)
│  (localhost)    │  ~70 msg/tick
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafka Consumer  │  Consumer group: iot-consumer-group
│ + ScyllaDB Buffer│  Aggregation window: 60 seconds
└────────┬────────┘
         │ Aggregated device snapshots
         ▼
┌─────────────────┐
│ Ollama          │  all-minilm:l6-v2 model
│ Embedding       │  Text → 384-dim vector
│ Generation      │  "RTU-001: supply_air=72F, return_air=68F..."
└────────┬────────┘
         │ Snapshot + embedding
         ▼
┌─────────────────────────────────────────────┐
│          ScyllaDB Cloud (Tyler-V3)          │
│                                             │
│  device_metrics_raw                         │
│  ├─ Raw time-series (30-day TTL)           │
│                                             │
│  metric_aggregation_buffer                  │
│  ├─ Temporary metric buffer (1-hour TTL)   │
│  └─ Replaces Redis for aggregation         │
│                                             │
│  device_state_snapshots                     │
│  ├─ Aggregated snapshots + embeddings      │
│  └─ Vector index: COSINE similarity         │
│                                             │
│  device_profiles                            │
│  ├─ Behavior fingerprints (centroid)       │
│  └─ Metric statistics (mean, std)          │
│                                             │
│  anomaly_events                             │
│  └─ Detected anomalies + scores            │
└────────┬───────────────┬────────────────────┘
         │               │
         ▼               ▼
  ┌────────────┐  ┌──────────────┐
  │ Dashboard  │  │  Anomaly     │
  │ (Dash UI)  │  │  Detection   │
  │            │  │  Engine      │
  │ localhost  │  │              │
  │ :8050      │  │ Continuous   │
  └────────────┘  └──────────────┘
```

### Component Interaction

1. **Producer** reads device configs, runs simulators, publishes to Kafka
2. **Consumer** subscribes to Kafka, aggregates in ScyllaDB buffer, generates embeddings, writes snapshots
3. **Profile Builder** reads historical snapshots, computes centroids, stores fingerprints
4. **Anomaly Detector** compares new snapshots to profiles, records events
5. **Dashboard** queries ScyllaDB, displays real-time device status

---

## Quick Start

### 1. Complete Data Pipeline (Phases 2 & 3 ✅)

Full pipeline: Device Simulators → Kafka → ScyllaDB Aggregation → Ollama Embeddings → ScyllaDB → Dashboard

```bash
# Start Kafka infrastructure (Zookeeper, Kafka)
./pipeline/start_local.sh

# Start producer (5 devices, 10-second intervals)
python pipeline/kafka_producer.py \
    --devices RTU-001:rooftop_unit MAU-001:makeup_air_unit \
              CH-001:chiller CT-001:cooling_tower AC-001:air_compressor \
    --interval 10

# Start consumer (aggregates, generates embeddings, writes to ScyllaDB)
python pipeline/kafka_consumer.py --aggregation-window 20

# Start dashboard (http://localhost:8050)
python dashboard/app.py
```

### 2. Device Fingerprinting & Anomaly Detection ✅

Build behavior profiles and detect anomalies using **three complementary approaches**:

```bash
# Build device fingerprints from historical data (required for all methods)
python pipeline/build_profiles.py

# Method 1: Rules-Based (hard limits + relationships)
python pipeline/detect_anomalies_rules.py --devices RTU-001
python pipeline/detect_anomalies_rules.py --continuous

# Method 2: Profile Similarity (COSINE distance to baseline)
python pipeline/detect_anomalies.py --devices RTU-001
python pipeline/detect_anomalies.py --continuous

# Method 3: Vector Search (ANN queries for historical context)
python pipeline/detect_anomalies_vector_search.py --devices RTU-001
python pipeline/detect_anomalies_vector_search.py --continuous

# Compare All Three Methods
python pipeline/detect_anomalies_all.py --device RTU-001

# Investigation Tools
python pipeline/find_similar_states.py --device RTU-001 --limit 5
./pipeline/test_anomaly_detection.sh
```

**The Three Paths**:

| Method | Speed | Use Case | Vector Search? |
|--------|-------|----------|----------------|
| **Rules-Based** | ⚡⚡⚡ Fastest | Safety limits, compliance | ❌ No |
| **Profile Similarity** | ⚡⚡ Fast | Production monitoring | ❌ No |
| **Vector Search** | ⚡ Good | Demos, novel detection | ✅ Yes |
| **All Three** | ⚡⚡ Fast | Comprehensive coverage | ✅ Yes |

**Key Differences**:
- `detect_anomalies_rules.py` = Traditional thresholds (50°F < temp < 90°F)
- `detect_anomalies.py` = ML-based deviation from baseline
- `detect_anomalies_vector_search.py` = ScyllaDB ANN queries + historical context
- `detect_anomalies_all.py` = Runs all three, shows consensus

See [THREE_DETECTION_PATHS.md](docs/THREE_DETECTION_PATHS.md) for complete comparison.

---

## System Components

### 1. Device Simulators (`iot_simulator/iot_simulator.py`)

**Purpose**: Generate realistic HVAC device metrics with stateful behavior.

**Device Types**:

| Device | Metrics | Key Characteristics |
|--------|---------|--------------------|
| **Rooftop Unit (RTU)** | 14 | Heating/cooling, compressor cycling, fan control |
| **Makeup Air Unit (MAU)** | 14 | Fresh air supply, mixing dampers, dual coils |
| **Chiller** | 15 | Chilled water production, efficiency tracking |
| **Cooling Tower** | 14 | Heat rejection, evaporative cooling, water flow |
| **Air Compressor** | 14 | Compressed air, pressure control, cooling |

**Key Features**:
- Stateful metric evolution (smooth transitions, realistic correlations)
- Configurable anomaly injection (rate, target devices)
- Device-specific operating ranges and relationships
- Support for multiple device instances

**Example Metric Correlations**:
- RTU: `compressor_status` → affects `power_consumption`, `supply_air_temp`
- Chiller: `capacity_percentage` → correlates with `power_consumption`, `delta_T`
- All devices: Operating conditions drift naturally over time

### 2. Kafka Producer (`pipeline/kafka_producer.py`)

**Purpose**: Stream device metrics to Kafka topic in real-time.

**Message Format**:
```json
{
  "device_id": "RTU-001",
  "device_type": "rooftopunit",
  "timestamp": "2025-11-18T15:30:00Z",
  "metric_name": "supply_air_temp",
  "metric_value": 72.5,
  "unit": "°F",
  "location": "building-A",
  "building_id": "bldg-001"
}
```

**Partitioning**: Uses `device_id` as key → keeps device metrics ordered

**Configuration**:
- Topic: `iot-metrics` (3 partitions)
- Compression: gzip
- Batch size: 16KB
- Linger: 10ms

### 3. Kafka Consumer (`pipeline/kafka_consumer.py`)

**Purpose**: Aggregate metrics, generate embeddings, write to ScyllaDB.

**Processing Pipeline**:

```
Kafka Message → ScyllaDB Buffer → Aggregation Window → Embedding → ScyllaDB
                 (60 seconds)        (all metrics)      (Ollama)   (2 inserts)
```

**Aggregation Logic**:
1. Buffer metrics per device in ScyllaDB `metric_aggregation_buffer` table (TTL: 1 hour)
2. Every 60 seconds (configurable), collect all metrics for each device
3. Convert to natural language: `"RTU-001 rooftopunit: supply air 72.5°F, return air 68.3°F, ..."`
4. Generate embedding via Ollama API
5. Write both raw metrics and snapshot to ScyllaDB
6. Delete buffer after processing

**Consumer Group**: `iot-consumer-group` (enables horizontal scaling)

### 4. Profile Builder (`pipeline/build_profiles.py`)

**Purpose**: Create device behavior fingerprints from historical data.

**Algorithm**:
```python
# 1. Query historical snapshots (exclude anomalies)
snapshots = get_device_snapshots(device_id, days_back=1)

# 2. Extract embeddings
embeddings = [s['embedding'] for s in snapshots]  # List of 384-dim vectors

# 3. Compute centroid (average)
profile_embedding = np.mean(embeddings, axis=0)  # Single 384-dim vector

# 4. Compute metric statistics
for metric in metrics:
    stats[metric] = {
        'mean': mean(values),
        'std': std(values),
        'min': min(values),
        'max': max(values)
    }

# 5. Store in device_profiles table
save_profile(device_id, profile_embedding, stats)
```

**Requirements**:
- Minimum 5 snapshots (default)
- Excludes already-flagged anomalies
- Configurable lookback period (default: 1 day)

### 5. Anomaly Detection - Three Complementary Approaches

#### A. Rules-Based Detector (`pipeline/detect_anomalies_rules.py`) 🔧

**Purpose**: Traditional threshold monitoring with domain knowledge.

**How it works**:
```python
# Hard thresholds per device type
DEVICE_RULES = {
    'rooftop_unit': {
        'supply_air_temp': {'min': 50, 'max': 90, 'critical': True},
        'power_consumption': {'min': 0, 'max': 50, 'critical': True}
    }
}

# Relationship rules
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
- ⚡⚡⚡ Fastest (<1ms)
- Deterministic and explainable
- No training required
- Catches safety violations

**Cons**:
- Requires domain expertise
- Brittle, doesn't adapt
- High maintenance

**Best for**: Safety limits, compliance, known failures

#### B. Profile Similarity Detector (`pipeline/detect_anomalies.py`) 🎯

**Purpose**: Fast ML-based detection using pre-computed baselines.

**Detection Methods**:

**Vector Similarity (Primary)**:
```python
# Compare snapshot embedding to profile embedding
similarity = cosine_similarity(snapshot_embedding, profile_embedding)

if similarity < 0.85:  # Configurable threshold
    flag_as_anomaly()
```

**Interpretation**:
- 1.0 = Identical behavior
- 0.85-1.0 = Normal variation
- < 0.85 = Anomalous behavior

**Statistical Outliers (Secondary)**:
```python
# Check each metric individually
for metric, value in snapshot.metrics:
    z_score = abs((value - profile_stats[metric]['mean']) / profile_stats[metric]['std'])
    
    if z_score > 3.0:  # 3-sigma rule
        flag_metric_as_outlier(metric, z_score)
```

**Pros**:
- ⚡⚡ Fast (~2ms)
- Captures holistic behavior
- No manual tuning
- Production-ready

**Cons**:
- Requires training data
- Doesn't use vector search
- Single profile limitation

**Best for**: 24/7 monitoring, deviation detection

**Note**: This approach computes similarity in Python (NumPy), not using ScyllaDB's vector index.

#### C. Vector Search Detector (`pipeline/detect_anomalies_vector_search.py`) 🔎

**Purpose**: Leverage ScyllaDB Vector Search for novel anomaly detection.

**How It Works**:

1. **For each snapshot, query similar historical states**:
   ```sql
   SELECT device_id, snapshot_time, embedding, metrics, is_anomalous
   FROM device_state_snapshots
   WHERE device_id = ? AND date = ?
   ORDER BY embedding ANN OF <snapshot_embedding>  ← Uses vector index!
   LIMIT 10
   ```

2. **Count similar normal snapshots**:
   ```python
   similar_normal = sum(1 for s in results if not s['is_anomalous'])
   ```

3. **Detection logic** (inverted from profile-based):
   ```python
   is_anomalous = (
       similar_normal_count < 3 or         # Novel behavior!
       profile_similarity < 0.85 or        # Different from profile
       len(outlier_metrics) > 0            # Statistical outlier
   )
   ```

**Key Insight**: 
- **Many similar normal snapshots** = Device behaving normally (seen this before)
- **Few/no similar normal snapshots** = Anomalous! (novel behavior, never seen this)

**Pros**:
- ⚡ Good (~10ms)
- Detects novel patterns
- Uses ScyllaDB vector index
- Historical context
- Demo-worthy!

**Cons**:
- Slower than other methods
- Requires significant history
- Partition key constraints

**Best for**: Demos, novel detection, root cause analysis

**When to use**:
- re:Invent presentations (showcases vector search)
- Investigation and analysis
- "Has this happened before?" queries

#### D. Unified Comparison (`pipeline/detect_anomalies_all.py`) 📊

**Purpose**: Run all three detection methods and compare results.

**What it does**:
- Executes Rules + Profile + Vector Search on same snapshots
- Shows detection rate for each method
- Calculates consensus (2+ methods agree)
- Identifies disagreements for tuning

**Example**:
```bash
python pipeline/detect_anomalies_all.py --device RTU-001
```

**Output**:
```
Method                Anomalies       Detection Rate
--------------------- --------------- ---------------
1. Rules-Based        2               40.0%
2. Profile Similarity 1               20.0%
3. Vector Search      3               60.0%

Consensus (2+ methods) 1              20.0%

Analysis:
 • Rules-based: Fast, deterministic, catches threshold violations
 • Profile similarity: Detects deviation from typical behavior
 • Vector search: Finds novel patterns, historical context
 💡 Vector search catching more → Novel behavior patterns
```

**Best for**: Demos, tuning thresholds, understanding coverage

#### E. Similarity Search Tool (`pipeline/find_similar_states.py`) 🔍

**Purpose**: Find historical device states similar to a given snapshot.

**Use cases**:
- "Has this anomaly happened before?"
- Root cause analysis
- Pattern recognition
- Incident investigation

**Example**:
```bash
python pipeline/find_similar_states.py --device RTU-001 --limit 5
```

**Output**:
- Top K most similar historical states
- Similarity scores (COSINE)
- Metric comparisons
- Analysis: "This anomaly occurred 3 times before"

### 6. Dashboard (`dashboard/app.py`)

**Purpose**: Real-time visualization of device status.

**Features**:
- Device status cards (5 devices)
- Key metrics display (6 per device)
- Color-coded status indicators (green = online)
- Statistics bar (total devices, online, anomalies, snapshots)
- Auto-refresh every 5 seconds

**Query Strategy**:
- Queries specific devices with partition keys (efficient)
- Fetches latest snapshot per device
- Avoids expensive DISTINCT queries

---

## Anomaly Detection Logic

### Two Detection Approaches

This system implements **two complementary approaches** for anomaly detection:

#### Approach 1: Profile Comparison (Fast)
- **File**: `detect_anomalies.py`
- **Method**: Compare snapshot to pre-computed profile centroid
- **Vector Search**: ❌ No (uses NumPy for similarity)
- **Best for**: Production monitoring, real-time alerts

#### Approach 2: Historical Similarity Search (Powerful)
- **File**: `detect_anomalies_vector_search.py`
- **Method**: Query similar historical states using ANN
- **Vector Search**: ✅ Yes (uses `ORDER BY embedding ANN OF`)
- **Best for**: Demos, investigations, novel anomaly detection

### Why Vector Embeddings?

Traditional threshold-based alerts require:
- Manual threshold tuning per metric
- Knowledge of all metric relationships
- Frequent updates as conditions change

**Vector embeddings capture**:
- Holistic device state (all metrics combined)
- Metric correlations automatically
- Operating mode patterns
- Temporal behavior patterns

### Fingerprint Creation

**Concept**: A device's "normal" behavior is the **centroid** of all normal operation embeddings.

```
Normal Snapshots (embeddings):
  [0.12, -0.34, 0.56, ...]  ← Morning operation
  [0.11, -0.35, 0.54, ...]  ← Afternoon operation  
  [0.13, -0.33, 0.57, ...]  ← Evening operation
  ...

Profile (centroid):
  [0.12, -0.34, 0.56, ...]  ← "Average" normal behavior
```

**Properties**:
- Represents typical device state across conditions
- Automatically adapts to seasonal/load variations (if retrained)
- More robust than single threshold per metric

### Similarity Calculation

**COSINE Similarity** measures angle between vectors:

```
similarity = (A · B) / (||A|| × ||B||)

Where:
  A = snapshot embedding
  B = profile embedding
  · = dot product
  ||·|| = vector magnitude
```

**Why COSINE?**
- Direction matters more than magnitude
- Normalized (always between -1 and 1)
- Fast to compute with vector index
- Works well for text-derived embeddings

### Vector Search Detection Logic (Inverted)

The vector search approach works by finding **similar historical states**:

```
New Snapshot
     ↓
  Query: "ORDER BY embedding ANN OF <snapshot_vector> LIMIT 10"
     ↓
Returns: 10 most similar historical snapshots
     ↓
Count: How many were NORMAL (not anomalous)?
     ↓
┌────────────────────────────────────────┐
│ Many similar normal states (7+)            │ → NORMAL behavior
│ Few similar normal states (< 3)            │ → ANOMALOUS! (novel)
└────────────────────────────────────────┘
```

**Key Insight**: 
- Vector search finds what's **similar** (not different)
- If current state is similar to many normal states → It's normal
- If current state has no similar normal states → It's novel/anomalous

**Example**:
```
Scenario A: Normal Operation
  Query: Current RTU snapshot
  Results: 8 similar snapshots found
           └─ 7 were normal, 1 was anomalous
  Decision: NORMAL (7 > 3 threshold)

Scenario B: Anomalous Operation  
  Query: Current RTU snapshot (compressor stuck)
  Results: 10 similar snapshots found
           └─ All 10 were also anomalous
  Decision: ANOMALOUS (0 < 3 threshold)
  Insight: "This problem has occurred 10 times before!"

Scenario C: Novel Anomaly
  Query: Current RTU snapshot (new failure mode)
  Results: 2 similar snapshots found
           └─ Both were normal but only loosely similar
  Decision: ANOMALOUS (2 < 3 threshold)
  Insight: "This is a NEW type of failure!"
```

### Anomaly Scenarios

| Scenario | Detection Method | Example |
|----------|------------------|----------|
| **Holistic deviation** | Vector similarity | Device running in wrong mode (cooling when should heat) |
| **Single metric spike** | Statistical outlier | Temperature sensor reads -999°C |
| **Multiple correlated changes** | Vector similarity | Compressor+fan+power all drift together |
| **Gradual degradation** | Vector similarity over time | Efficiency slowly decreasing |
| **Sensor failure** | Statistical outlier | Stuck at constant value |
| **Novel failure mode** | Vector search (few similar) | Never-seen-before behavior pattern |

### Threshold Tuning

**Similarity Threshold** (default: 0.85)
- **Too high (0.95)**: Many false positives, normal variation flagged
- **Too low (0.70)**: Miss subtle anomalies, only catch major issues
- **Recommended**: Start at 0.85, adjust based on false positive rate

**Sigma Threshold** (default: 3.0)
- **Standard 3-sigma**: ~99.7% of normal data within bounds
- **Stricter (2.5)**: Catch more outliers, more false positives
- **Looser (4.0)**: Only extreme outliers

---

## Data Flow

### 1. Metric Generation & Streaming

```
Device Simulator                Kafka
================                =====

Every 10 seconds:

  tick()                    
    ↓
  14-15 metrics              → Topic: iot-metrics
    ↓                        → Partition: hash(device_id)
  For each metric:           → Message: JSON
    ↓
    send_to_kafka()

Result: ~70 messages per tick (5 devices × 14 metrics)
```

### 2. Aggregation & Embedding

```
Kafka Consumer          ScyllaDB Buffer         Ollama              ScyllaDB
==============          ===============         ======              ========

Continuous:

  poll_kafka()               
    ↓                        
  For each message:          
    ↓                        
    write_to_buffer()  ─────→ metric_aggregation_buffer
                               TTL: 1 hour

Every 60 seconds:

  check_ready_devices()
    ↓
  For each device:
    ↓
    fetch_window() ─────────→ SELECT * FROM buffer
    ↓                         WHERE device_id = ?
    create_text()              AND window_start = ?
      "RTU-001: temp=72F, ..."
    ↓
    generate_embedding() ──→ POST /api/embeddings
    ↓                                ↓
    write_to_scylla() ────────→ 384-dim vector
      - device_state_snapshots      ↓
      - DELETE FROM buffer ←──────┘
```

### 3. Profile Building

```
Profile Builder                ScyllaDB
===============                ========

One-time or periodic:

  For each device:
    ↓
    query_snapshots() ───────→ SELECT * FROM device_state_snapshots
                               WHERE device_id = ? AND date = ?
                               AND is_anomalous = false
    ↓
    embeddings[] ←──────────── [embedding1, embedding2, ...]
    ↓
    profile = mean(embeddings)
    ↓
    stats = compute_stats(metrics)
    ↓
    save_profile() ──────────→ INSERT INTO device_profiles
                               (device_id, profile_embedding, metric_stats)
```

### 4. Anomaly Detection

```
Anomaly Detector               ScyllaDB
================               ========

Continuous (every 30s):

  For each device:
    ↓
    load_profile() ──────────→ SELECT * FROM device_profiles
    ↓                          WHERE device_id = ?
    get_recent_snapshots() ──→ SELECT * FROM device_state_snapshots
    ↓                          WHERE device_id = ? AND date = ?
    For each snapshot:         AND snapshot_time >= ?
      ↓
      similarity = cosine(snapshot.embedding, profile.embedding)
      ↓
      outliers = check_z_scores(snapshot.metrics, profile.stats)
      ↓
      if similarity < 0.85 OR outliers:
        ↓
        record_anomaly() ────→ INSERT INTO anomaly_events
        ↓                      UPDATE device_state_snapshots
        log_event()            SET is_anomalous = true
```

---

## Operations Guide

### Initial Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with ScyllaDB credentials
   ```

3. **Start Ollama**
   ```bash
   ollama pull all-minilm:l6-v2
   ollama serve  # Port 11434
   ```

4. **Initialize ScyllaDB Schema**
   ```bash
   python scylladb_setup/create_iot_schema.py
   ```

### Daily Operations

**Terminal 1: Infrastructure**
```bash
./pipeline/start_local.sh
# Kafka UI: http://localhost:8080
```

**Terminal 2: Producer**
```bash
python pipeline/kafka_producer.py \
    --devices RTU-001:rooftop_unit MAU-001:makeup_air_unit \
              CH-001:chiller CT-001:cooling_tower AC-001:air_compressor \
    --interval 10
```

**Terminal 3: Consumer**
```bash
python pipeline/kafka_consumer.py --aggregation-window 60
```

**Terminal 4: Dashboard**
```bash
python dashboard/app.py
# Dashboard: http://localhost:8050
```

**Terminal 5: Anomaly Detection** (after profiles built)
```bash
python pipeline/detect_anomalies.py --continuous
```

### Profile Management

**Initial Profile Creation** (run after 5+ minutes of data collection):
```bash
python pipeline/build_profiles.py
```

**Update Profiles** (after device maintenance or behavior changes):
```bash
python pipeline/build_profiles.py --devices RTU-001 --days-back 7
```

**Verify Profiles**:
```bash
python -c "
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv

load_dotenv()
auth = PlainTextAuthProvider(
    username=os.getenv('SCYLLA_USERNAME'),
    password=os.getenv('SCYLLA_PASSWORD')
)
cluster = Cluster(
    os.getenv('SCYLLA_HOSTS').split(','),
    port=int(os.getenv('SCYLLA_PORT', '19042')),
    auth_provider=auth
)
session = cluster.connect('iot_monitoring')
result = session.execute('SELECT device_id, device_type, profile_created_at FROM device_profiles')
for row in result:
    print(f'{row.device_id} ({row.device_type}) - {row.profile_created_at}')
cluster.shutdown()
"
```

### Monitoring

**Check Data Flow**:
```bash
python pipeline/verify_data.py
```

**Kafka Consumer Lag**:
```bash
docker exec -it kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group iot-consumer-group \
    --describe
```

**ScyllaDB Buffer**:
```bash
python -c "
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv

load_dotenv()
auth = PlainTextAuthProvider(
    username=os.getenv('SCYLLA_USERNAME'),
    password=os.getenv('SCYLLA_PASSWORD')
)
cluster = Cluster(
    os.getenv('SCYLLA_HOSTS').split(','),
    port=int(os.getenv('SCYLLA_PORT', '19042')),
    auth_provider=auth
)
session = cluster.connect('iot_monitoring')
result = session.execute('SELECT device_id, window_start, count(*) FROM metric_aggregation_buffer GROUP BY device_id, window_start ALLOW FILTERING')
for row in result:
    print(f'{row.device_id} @ {row.window_start}: {row.count} metrics')
cluster.shutdown()
"
```

### Testing

**Test with Anomaly Injection**:
```bash
# Automated test script
./pipeline/test_anomaly_detection.sh

# Manual injection
python pipeline/kafka_producer.py \
    --devices RTU-001:rooftop_unit \
    --anomaly-devices RTU-001 \
    --interval 10 \
    --duration 120
```

### Shutdown

```bash
# Stop Python processes
pkill -f kafka_producer.py
pkill -f kafka_consumer.py
pkill -f dashboard/app.py
pkill -f detect_anomalies.py

# Stop Docker
docker-compose -f pipeline/docker-compose.yml down
```

---

## Current Status

### Completed Phases

#### Phase 1: Device Simulators ✅
- ✅ All 5 HVAC device types implemented (RTU, MAU, Chiller, Cooling Tower, Air Compressor)
- ✅ Stateful metrics with realistic correlations
- ✅ Anomaly injection support

#### Phase 2: Data Pipeline ✅
- ✅ Kafka infrastructure (local Docker)
- ✅ Producer: Streams individual metrics to Kafka
- ✅ Consumer: ScyllaDB aggregation → Ollama embeddings → ScyllaDB
- ✅ ScyllaDB schema with vector indexes and aggregation buffer (COSINE similarity)
- ✅ 384-dim embeddings via Ollama all-minilm:l6-v2

#### Phase 3: Anomaly Detection ✅
- ✅ **Three detection approaches** implemented and tested
  - Rules-Based: Hard limits + relationship rules
  - Profile Similarity: COSINE distance to baseline
  - Vector Search: ANN queries for historical context
- ✅ Device fingerprinting (profile embeddings = centroid of normal behavior)
- ✅ Unified comparison tool (runs all three, shows consensus)
- ✅ Statistical outlier detection (z-scores)
- ✅ Anomaly event recording in ScyllaDB
- ✅ Continuous monitoring mode for all methods

#### Phase 4: Dashboard ✅
- ✅ Real-time Dash UI (http://localhost:8050)
- ✅ Device status cards with key metrics
- ✅ Auto-refresh (5 seconds)
- ⏳ Anomaly visualization (pending integration)

### Next Phase
- Phase 5: AWS deployment (Fargate, Kinesis)

## Configuration

### Environment Variables (`.env`)

```bash
# ScyllaDB Cloud Connection
SCYLLA_HOSTS=node1.scylladb.com,node2.scylladb.com
SCYLLA_PORT=19042
SCYLLA_USERNAME=your_username
SCYLLA_PASSWORD=your_password
SCYLLA_KEYSPACE=iot_monitoring

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-metrics

# Ollama Configuration
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=all-minilm:l6-v2
```

### ScyllaDB Schema

**Cluster**: Tyler-V3 (2025.4.0 RC)
**Keyspace**: `iot_monitoring` (RF=3, NetworkTopologyStrategy)

#### Tables

**1. device_metrics_raw**
```sql
CREATE TABLE device_metrics_raw (
    device_id text,
    date text,
    timestamp timestamp,
    metric_name text,
    metric_value double,
    unit text,
    device_type text STATIC,
    location text STATIC,
    building_id text STATIC,
    PRIMARY KEY ((device_id, date), timestamp, metric_name)
) WITH CLUSTERING ORDER BY (timestamp DESC, metric_name ASC)
  AND default_time_to_live = 2592000;  -- 30 days
```

**2. metric_aggregation_buffer**
```sql
CREATE TABLE metric_aggregation_buffer (
    device_id text,
    window_start timestamp,
    metric_name text,
    metric_value double,
    device_type text STATIC,
    location text STATIC,
    building_id text STATIC,
    last_updated timestamp,
    PRIMARY KEY ((device_id, window_start), metric_name)
) WITH CLUSTERING ORDER BY (metric_name ASC)
  AND default_time_to_live = 3600;  -- 1 hour (replaces Redis)
```

**3. device_state_snapshots**
```sql
CREATE TABLE device_state_snapshots (
    device_id text,
    date text,
    snapshot_time timestamp,
    metrics map<text, double>,
    embedding vector<float, 384>,
    embedding_method text,
    is_anomalous boolean,
    anomaly_score double,
    device_type text STATIC,
    location text STATIC,
    building_id text STATIC,
    PRIMARY KEY ((device_id, date), snapshot_time)
) WITH CLUSTERING ORDER BY (snapshot_time DESC)
  AND default_time_to_live = 7776000;  -- 90 days

-- Vector index for similarity search
CREATE CUSTOM INDEX device_state_embedding_idx 
ON device_state_snapshots(embedding) 
USING 'StorageAttachedIndex';
```

**4. device_profiles**
```sql
CREATE TABLE device_profiles (
    device_id text PRIMARY KEY,
    device_type text,
    location text,
    building_id text,
    profile_embedding vector<float, 384>,
    metric_stats map<text, frozen<map<text, double>>>,
    profile_created_at timestamp,
    profile_updated_at timestamp,
    last_seen timestamp
);
```

**5. anomaly_events**
```sql
CREATE TABLE anomaly_events (
    device_id text,
    detected_at timestamp,
    anomaly_score double,
    anomaly_type text,
    metrics_snapshot map<text, text>,
    PRIMARY KEY (device_id, detected_at)
) WITH CLUSTERING ORDER BY (detected_at DESC);
```

### Kafka Configuration

**Topic**: `iot-metrics`
- **Partitions**: 3
- **Replication Factor**: 1 (local development)
- **Retention**: 7 days
- **Compression**: gzip

**Consumer Group**: `iot-consumer-group`
- **Auto Offset Reset**: earliest
- **Enable Auto Commit**: true
- **Max Poll Records**: 500

### Device Fleet Configuration

Current deployment (`pipeline/fleet_config.json`):

```json
{
  "devices": [
    {"device_id": "RTU-001", "device_type": "rooftop_unit", "location": "building-A", "building_id": "bldg-001"},
    {"device_id": "MAU-001", "device_type": "makeup_air_unit", "location": "building-A", "building_id": "bldg-001"},
    {"device_id": "CH-001", "device_type": "chiller", "location": "building-B", "building_id": "bldg-002"},
    {"device_id": "CT-001", "device_type": "cooling_tower", "location": "building-B", "building_id": "bldg-002"},
    {"device_id": "AC-001", "device_type": "air_compressor", "location": "building-C", "building_id": "bldg-003"}
  ]
}
```

### Tunable Parameters

| Parameter | Default | Location | Description |
|-----------|---------|----------|-------------|
| `--interval` | 60s | Producer | Time between metric emissions |
| `--aggregation-window` | 60s | Consumer | Metric aggregation window |
| `--similarity-threshold` | 0.85 | Detector | COSINE similarity for anomaly |
| `--sigma-threshold` | 3.0 | Detector | Z-score for outlier detection |
| `--days-back` | 1 | Profile Builder | Historical data for profiling |
| `--min-snapshots` | 5 | Profile Builder | Minimum data for profile |

---

## Troubleshooting

### No Data in Dashboard

**Symptom**: Dashboard shows "0 snapshots" or blank device cards

**Causes & Solutions**:

1. **Consumer not running**
   ```bash
   ps aux | grep kafka_consumer
   # If not running:
   python pipeline/kafka_consumer.py --aggregation-window 20
   ```

2. **Insufficient aggregation time**
   - Wait 60+ seconds after starting producer
   - Check consumer logs for "Writing snapshot" messages

3. **ScyllaDB buffer empty**
   - Query `metric_aggregation_buffer` table
   - Should show active aggregation windows

4. **Query using wrong date**
   - Dashboard queries current date only
   - Verify device clocks are synchronized

### Anomaly Detection Not Working

**Symptom**: `detect_anomalies.py` finds no anomalies or no profiles

**Solutions**:

1. **No profiles exist**
   ```bash
   python pipeline/build_profiles.py
   ```

2. **Insufficient historical data**
   - Need at least 5 snapshots per device
   - Run producer for 2+ minutes before building profiles

3. **All data flagged as anomalous**
   - Threshold too strict
   - Lower similarity threshold:
     ```bash
     python pipeline/detect_anomalies.py --similarity-threshold 0.75
     ```

### Kafka Connection Errors

**Symptom**: `KafkaError: NoBrokersAvailable`

**Solutions**:

1. **Kafka not running**
   ```bash
   docker ps | grep kafka
   # If not running:
   ./pipeline/start_local.sh
   ```

2. **Port conflict**
   ```bash
   lsof -i :9092
   # Kill conflicting process or change port
   ```

3. **Docker network issues**
   ```bash
   docker-compose -f pipeline/docker-compose.yml down
   docker network prune
   ./pipeline/start_local.sh
   ```

### ScyllaDB Connection Issues

**Symptom**: `NoHostAvailable` or authentication errors

**Solutions**:

1. **Check credentials**
   ```bash
   cat .env | grep SCYLLA
   # Verify username/password
   ```

2. **Test connection**
   ```bash
   python scylladb_setup/check_setup.py
   ```

3. **Firewall/network**
   - Verify port 19042 is accessible
   - Check ScyllaDB Cloud cluster status
   - Whitelist your IP in Cloud console

### Ollama Errors

**Symptom**: `Connection refused` or embedding generation fails

**Solutions**:

1. **Ollama not running**
   ```bash
   curl http://localhost:11434/api/tags
   # If fails:
   ollama serve
   ```

2. **Model not pulled**
   ```bash
   ollama pull all-minilm:l6-v2
   ```

3. **Wrong model name**
   - Check `config.yaml` matches installed model

### High Consumer Lag

**Symptom**: Consumer can't keep up with producer

**Solutions**:

1. **Scale consumers**
   ```bash
   # Start multiple consumers (same group)
   python pipeline/kafka_consumer.py &
   python pipeline/kafka_consumer.py &
   ```

2. **Increase aggregation window**
   ```bash
   python pipeline/kafka_consumer.py --aggregation-window 30
   ```

3. **Reduce producer rate**
   ```bash
   python pipeline/kafka_producer.py --interval 20  # Slower
   ```

### Memory Issues

**Symptom**: Out of memory errors, slow performance

**Solutions**:

1. **ScyllaDB buffer table**
   - TTL automatically expires old windows
   - Monitor `metric_aggregation_buffer` table size

2. **Consumer batch size**
   - Reduce `max_poll_records` in consumer config

3. **Clear old data**
   ```bash
   # ScyllaDB uses TTL automatically for all tables
   # Buffer table: 1 hour TTL
   # Raw metrics: 30 day TTL
   # Snapshots: 90 day TTL
   ```

### Dashboard Performance

**Symptom**: Slow page loads, timeouts

**Solutions**:

1. **Increase refresh interval**
   - Edit `dashboard/app.py`, change `Interval` to 10000 (10 seconds)

2. **Optimize queries**
   - Dashboard already uses partition keys
   - Avoid scanning entire tables

3. **Reduce device count**
   - Query fewer devices in parallel

---

## Development

### Project Structure Deep Dive

```
reinvent_demo/
├── iot_simulator/
│   ├── iot_simulator.py      # 650 lines, 5 device types
│   └── test_fleet.sh         # Test harness
│
├── pipeline/
│   ├── kafka_producer.py     # 286 lines, streaming logic
│   ├── kafka_consumer.py     # 509 lines, ScyllaDB aggregation + embeddings
│   ├── build_profiles.py     # 304 lines, fingerprinting
│   ├── detect_anomalies.py   # 420 lines, anomaly detection
│   ├── verify_data.py        # Data validation
│   └── docker-compose.yml    # Local Kafka infrastructure
│
├── dashboard/
│   └── app.py                # 380 lines, Dash UI
│
├── scylladb_setup/
│   ├── create_iot_schema.py  # IoT schema initialization
│   └── check_setup.py        # Connection testing
│
└── docs/
    ├── THREE_DETECTION_PATHS.md  # Complete comparison
    ├── DETECTION_COMPARISON.md   # Quick reference
    ├── VECTOR_SEARCH_USAGE.md    # Usage guide
    └── device_specifications.md  # Device metrics
```

### Local Development Workflow

1. **Make changes** to simulators, pipeline, or dashboard
2. **Test individually**:
   ```bash
   # Test simulator
   python iot_simulator/iot_simulator.py --device-type rooftop_unit --device-id test-001 --duration 30
   
   # Test profile builder
   python pipeline/build_profiles.py --devices RTU-001
   
   # Test detector
   python pipeline/detect_anomalies.py --devices RTU-001 --no-record
   ```
3. **Run end-to-end**:
   ```bash
   ./pipeline/test_anomaly_detection.sh
   ```

### Adding New Device Types

1. **Define device class** in `iot_simulator/iot_simulator.py`:
   ```python
   class NewDeviceType(DeviceSimulator):
       def __init__(self, device_id, location, building_id):
           super().__init__(device_id, "newdevice", location, building_id)
           self.define_metrics()
   ```

2. **Add to DEVICE_TYPES** dict

3. **Update fleet config** in `pipeline/fleet_config.json`

4. **Rebuild profiles** after data collection

### Performance Testing

```bash
# Simulate larger fleet
python pipeline/kafka_producer.py \
    --fleet-config large_fleet.json \
    --interval 10

# Monitor throughput
watch -n 1 'docker exec -it kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group iot-consumer-group \
    --describe'
```

---

## References

### Documentation

- **[THREE_DETECTION_PATHS.md](docs/THREE_DETECTION_PATHS.md)** - Comprehensive comparison of all detection methods
- **[DETECTION_COMPARISON.md](docs/DETECTION_COMPARISON.md)** - Quick reference guide
- **[VECTOR_SEARCH_USAGE.md](docs/VECTOR_SEARCH_USAGE.md)** - Vector search usage guide
- **[QUICKSTART.md](QUICKSTART.md)** - Quick reference for common commands
- **[device_specifications.md](docs/device_specifications.md)** - HVAC device metrics reference

### External Resources

- **[ScyllaDB Vector Search](https://cloud.docs.scylladb.com/stable/vector-search/index.html)** - Official documentation
- **[Ollama Embeddings](https://ollama.ai/)** - Local embedding generation
- **[Kafka Documentation](https://kafka.apache.org/documentation/)** - Message streaming
- **[Dash by Plotly](https://dash.plotly.com/)** - Dashboard framework

### Key Concepts

**Vector Search**:
- Storage-Attached Indexes (SAI) in ScyllaDB
- COSINE similarity for semantic matching
- ANN (Approximate Nearest Neighbor) queries

**Embeddings**:
- Text-to-vector transformation (all-minilm:l6-v2)
- 384-dimensional semantic space
- Captures device behavior holistically

**Anomaly Detection**:
- Centroid-based fingerprinting
- Multi-modal detection (vector + statistical)
- Adaptive thresholds

### Architecture Patterns

**Event Streaming**:
- Kafka for message buffering and ordering
- Consumer groups for horizontal scaling
- At-least-once delivery semantics

**Time-Series Storage**:
- ScyllaDB partition keys by (device_id, date)
- Clustering by timestamp DESC
- Automatic TTL for data lifecycle

**Aggregation**:
- ScyllaDB as temporary buffer (metric_aggregation_buffer table)
- Time-window aggregation (60s)
- Batch embedding generation
- Auto-expiring with TTL (1 hour)

### Production Considerations

**Scalability**:
- Kafka partitions scale to 1000s of devices
- ScyllaDB handles millions of writes/sec
- Consumer groups enable horizontal scaling
- Vector indexes remain fast at large scale

**Reliability**:
- ScyllaDB RF=3 for high availability
- Kafka replication for message durability
- Consumer offset tracking for exactly-once
- TTLs prevent unbounded growth

**Monitoring**:
- Kafka UI for stream health
- ScyllaDB Cloud metrics
- Consumer lag tracking
- Dashboard for business metrics

### Related Projects

- **Similar Incident Search**: Use vector index to find "devices that behaved like this before"
- **Predictive Maintenance**: Extend to predict failures before they occur
- **Energy Optimization**: Detect inefficient operation patterns
- **Cross-Device Correlation**: Find related anomalies across multiple devices

### Future Enhancements

1. **Adaptive Profiles**: Auto-retrain profiles periodically
2. **Anomaly Severity Scoring**: Classify by impact (critical, warning, info)
3. **Root Cause Analysis**: Use vector search to find similar past incidents
4. **Real-time Alerts**: Slack/email notifications via webhooks
5. **Multi-Tenant**: Support multiple buildings/fleets
6. **AWS Deployment**: Fargate + Kinesis + ScyllaDB Cloud
7. **Mobile App**: View alerts on mobile devices
8. **Historical Playback**: Replay past data for testing/training

### Contact & Support

For questions about this demo:
- ScyllaDB Community: https://scylladb.com/community
- GitHub Issues: https://github.com/scylladb/scylla/issues

### License

This demo code is provided as-is for educational and demonstration purposes.

---

## Acknowledgments

**Built with**:
- ScyllaDB Vector Search (2025.4.0 RC)
- Kafka ecosystem
- Ollama embeddings
- Python 3.9+

**Special thanks to**:
- ScyllaDB team for vector search capabilities
- Ollama team for local embedding generation
- Open source community

---

**Demo Version**: 1.0 (Phase 3 Complete)
**Last Updated**: November 2025
**Status**: ✅ Production-ready for demo purposes
