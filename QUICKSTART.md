# Quick Start Guide

## Prerequisites
- Docker (for Kafka infrastructure)
- Python 3.9+
- Ollama running with `all-minilm:l6-v2` model
- ScyllaDB Cloud cluster (Tyler-V3)
- `.env` file configured with ScyllaDB credentials

## System Architecture

```
Device Simulators → Kafka → Consumer → ScyllaDB Buffer → Ollama → ScyllaDB → Dashboard
                                              ↓                         ↓
                                         Aggregation              Profile/Detect
                                                                  Anomalies
```

## Step-by-Step Setup

### 1. Start Infrastructure (Terminal 1)
```bash
cd /Users/tdenton/Development/reinvent_demo
./pipeline/start_local.sh
```
**Starts**: Kafka, Zookeeper, Kafka UI (http://localhost:8080)

### 2. Start Producer (Terminal 2)
```bash
# Normal operation (all devices)
python pipeline/kafka_producer.py \
    --devices RTU-001:rooftop_unit MAU-001:makeup_air_unit \
              CH-001:chiller CT-001:cooling_tower AC-001:air_compressor \
    --interval 10

# With anomaly injection
python pipeline/kafka_producer.py \
    --devices RTU-001:rooftop_unit \
    --anomaly-devices RTU-001 \
    --interval 10
```

### 3. Start Consumer (Terminal 3)
```bash
python pipeline/kafka_consumer.py --aggregation-window 60
```
**Does**: Aggregates metrics in ScyllaDB buffer (60s) → Generates embeddings → Writes snapshots

### 4. Build Device Profiles (Once)
After devices have run for ~5+ minutes:
```bash
python pipeline/build_profiles.py
```
**Creates**: Behavior fingerprints in `device_profiles` table

### 5. Start Dashboard (Terminal 4)
```bash
python dashboard/app.py
```
**Access**: http://localhost:8050

### 6. Run Anomaly Detection
```bash
# One-time check
python pipeline/detect_anomalies.py

# Continuous monitoring
python pipeline/detect_anomalies.py --continuous
```

## Common Commands

### Check System Health
```bash
# Verify data is flowing
python pipeline/verify_data.py

# Check device profiles
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

### Kafka Management
```bash
# View topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Check consumer lag
docker exec -it kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group iot-consumer-group \
    --describe
```

### Rebuild Profiles
```bash
# After device behavior changes or maintenance
python pipeline/build_profiles.py --devices RTU-001 CH-001
```

### Test Anomaly Detection
```bash
# Full automated test (injects anomaly, detects, records)
./pipeline/test_anomaly_detection.sh
```

## Tuning Parameters

### Producer
- `--interval`: Seconds between metric emissions (default: 60)
- `--anomaly-devices`: Device IDs that should exhibit anomalies

### Consumer
- `--aggregation-window`: Seconds to aggregate metrics (default: 60)

### Profile Builder
- `--days-back`: Days of history to use (default: 1)
- `--min-snapshots`: Minimum snapshots required (default: 5)

### Anomaly Detection
- `--similarity-threshold`: COSINE similarity threshold (default: 0.85)
- `--sigma-threshold`: Z-score threshold for outliers (default: 3.0)
- `--hours-back`: Hours of recent snapshots to check (default: 1)
- `--continuous`: Run every 30 seconds

## Troubleshooting

### No snapshots in dashboard
- Check consumer is running: `ps aux | grep kafka_consumer`
- Verify consumer logs show embeddings being generated
- Check ScyllaDB buffer: Query `metric_aggregation_buffer` table

### Anomaly detection finds no profiles
```bash
python pipeline/build_profiles.py
```

### Kafka not working
```bash
# Restart infrastructure
docker-compose -f pipeline/docker-compose.yml down
./pipeline/start_local.sh
```

### ScyllaDB connection issues
- Verify `.env` has correct credentials
- Check cluster status in ScyllaDB Cloud console
- Test connection: `python scylladb_setup/check_setup.py`

## Monitoring URLs

- **Kafka UI**: http://localhost:8080
- **Dashboard**: http://localhost:8050
- **ScyllaDB**: Query tables directly via Python scripts

## Stop Everything

```bash
# Stop Python processes
pkill -f kafka_producer.py
pkill -f kafka_consumer.py
pkill -f dashboard/app.py
pkill -f detect_anomalies.py

# Stop Docker infrastructure
docker-compose -f pipeline/docker-compose.yml down
```

## Demo Workflow

### For Presentations
1. Start infrastructure (background)
2. Start producer with 5 devices (visible terminal)
3. Start consumer (visible terminal showing embeddings)
4. Open dashboard (http://localhost:8050)
5. Show real-time metrics flowing
6. Build profiles: `python pipeline/build_profiles.py`
7. Run detection: `python pipeline/detect_anomalies.py --continuous`
8. Inject anomaly: Start new producer with `--anomaly-devices`
9. Watch detection catch the anomaly in real-time
10. Show anomaly events in dashboard (pending) or via query

### For Development
```bash
# Quick test cycle
./pipeline/start_local.sh
python pipeline/kafka_producer.py --devices RTU-001:rooftop_unit --interval 10 &
python pipeline/kafka_consumer.py --aggregation-window 60 &
sleep 120  # Wait for aggregation window
python pipeline/build_profiles.py
python pipeline/detect_anomalies.py
```

## Next Steps

1. **Dashboard Integration**: Add anomaly visualization to UI
2. **Alerts**: Email/Slack notifications for critical anomalies
3. **Vector Search Demo**: Show "find similar incidents" using ANN queries
4. **AWS Deployment**: Move to production with Fargate + Kinesis
