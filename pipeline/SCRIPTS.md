# Pipeline Management Scripts

## Overview

This directory contains scripts to manage the IoT Anomaly Detection pipeline.

## Scripts

### `start_pipeline.sh`

**Purpose**: Start all pipeline services automatically

**What it does**:
1. Starts Kafka infrastructure (Docker)
2. Starts device simulators (producer)
3. Starts metric aggregator (consumer)
4. Waits 90 seconds for initial data collection
5. Builds device profiles
6. Starts continuous anomaly detection
7. Starts dashboard

**Usage**:
```bash
./pipeline/start_pipeline.sh
```

**Output**:
- Process IDs are saved to `logs/pids.txt`
- Logs are written to `logs/*.log`
- Dashboard: http://localhost:8050
- Kafka UI: http://localhost:8080

---

### `stop_pipeline.sh`

**Purpose**: Stop all pipeline services

**What it does**:
1. Reads PIDs from `logs/pids.txt` and stops those processes
2. Falls back to stopping by process name if needed
3. Stops Docker services (Kafka, Zookeeper)

**Usage**:
```bash
./pipeline/stop_pipeline.sh
```

---

### `check_status.sh`

**Purpose**: Check the health of all pipeline services

**What it does**:
1. Checks Docker services status
2. Checks Python processes (producer, consumer, detector, dashboard)
3. Tests ScyllaDB connectivity
4. Scans logs for recent errors
5. Displays service URLs

**Usage**:
```bash
./pipeline/check_status.sh
```

**Example output**:
```
🔍 IoT Pipeline Status Check
============================

🐳 Docker Services:
   ✅ kafka (Up 23 hours, healthy)
   ✅ zookeeper (Up 23 hours, healthy)
   ✅ kafka-ui (Up 23 hours)

🐍 Python Services:
   ✅ kafka_producer (PID: 4204)
   ✅ kafka_consumer (PID: 4560)
   ✅ detect_anomalies_vector_search (PID: 26341)
   ✅ app (PID: 20078)

💾 ScyllaDB:
   ✅ Connected - 2553 snapshots

📝 Recent Log Status:
   ✅ anomaly_detector: no recent errors

🌐 Service URLs:
   Dashboard:  http://localhost:8050
   Kafka UI:   http://localhost:8080
```

---

### `start_local.sh`

**Purpose**: Start only Kafka infrastructure (lightweight)

**What it does**:
- Starts Kafka, Zookeeper, and Kafka UI via Docker Compose
- Waits for Kafka to be ready
- Does NOT start Python services

**Usage**:
```bash
./pipeline/start_local.sh
```

Use this when you want to start services manually or one at a time.

---

## Log Files

All background services write to `logs/` directory:

- `logs/producer.log` - Device simulator output
- `logs/consumer.log` - Metric aggregator output
- `logs/anomaly_detector.log` - Continuous anomaly detection output
- `logs/dashboard.log` - Dashboard output
- `logs/pids.txt` - Process IDs for easy shutdown

**View logs in real-time**:
```bash
tail -f logs/consumer.log
tail -f logs/anomaly_detector.log
```

---

## Continuous Anomaly Detection

The `detect_anomalies_vector_search.py` script runs continuously in the background and:

- Checks for new snapshots every 30 seconds
- Only analyzes snapshots not yet marked (`--only-new` flag)
- Uses ScyllaDB Vector Search (ANN queries) for similarity detection
- Updates `is_anomalous` flag in `device_state_snapshots`
- Creates entries in `anomaly_events` table
- Detects anomalies when:
  - Profile similarity < 0.85 (configurable with `--similarity-threshold`)
  - Few similar normal snapshots found (< 3)
  - Statistical outliers detected (Z-score > 3.0)

**Manual invocation**:
```bash
# One-time check
python pipeline/detect_anomalies_vector_search.py --hours-back 1

# Continuous mode
python pipeline/detect_anomalies_vector_search.py --continuous --only-new
```

---

## Troubleshooting

### Services not starting
```bash
# Check what's running
./pipeline/check_status.sh

# View specific log
tail -50 logs/consumer.log
```

### Anomalies not detected
1. Check if profiles exist:
   ```bash
   python -c "
   from cassandra.cluster import Cluster
   from cassandra.auth import PlainTextAuthProvider
   import os
   from dotenv import load_dotenv
   load_dotenv()
   auth = PlainTextAuthProvider(username=os.getenv('SCYLLA_USERNAME'), password=os.getenv('SCYLLA_PASSWORD'))
   cluster = Cluster(os.getenv('SCYLLA_HOSTS').split(','), port=int(os.getenv('SCYLLA_PORT', '19042')), auth_provider=auth)
   session = cluster.connect('iot_monitoring')
   result = session.execute('SELECT device_id FROM device_profiles')
   print('Profiles:', [r.device_id for r in result])
   cluster.shutdown()
   "
   ```

2. Check detector is running:
   ```bash
   ps aux | grep detect_anomalies_vector_search
   ```

3. View detector logs:
   ```bash
   tail -f logs/anomaly_detector.log
   ```

### Port conflicts
If ports 8050 (dashboard) or 8080 (Kafka UI) are in use:
```bash
# Find what's using the port
lsof -i :8050
lsof -i :8080

# Kill the process or change ports in the scripts
```

---

## Architecture

```
Device Simulators (Producer)
         ↓
       Kafka
         ↓
Consumer (Aggregator + Embeddings)
         ↓
    ScyllaDB Buffer → Snapshots Table
         ↓                    ↓
    Profile Builder    Anomaly Detector (Continuous)
         ↓                    ↓
    device_profiles    anomaly_events + is_anomalous flag
                            ↓
                       Dashboard (Real-time)
```

The continuous anomaly detector ensures that as new snapshots arrive, they are immediately analyzed and marked, so the dashboard always shows current anomaly status.
