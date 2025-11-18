# IoT Data Pipeline

Local Kafka-based pipeline for streaming IoT device metrics to ScyllaDB.

## Architecture

```
[IoT Simulators] → [Kafka] → [State Aggregator] → [Embedding Service] → [ScyllaDB]
                       ↓
                  [Kafka UI]
```

## Quick Start

### 1. Start Kafka Infrastructure

```bash
./pipeline/start_local.sh
```

This starts:
- Kafka broker (localhost:9092)
- Zookeeper (localhost:2181)
- Kafka UI (http://localhost:8080)

### 2. Install Python Dependencies

```bash
pip install -r pipeline/requirements.txt
```

### 3. Start Producer

**Option A: Multiple devices from config**
```bash
python pipeline/kafka_producer.py \
  --fleet-config pipeline/fleet_config.json \
  --interval 10 \
  --anomaly-devices RTU-003 CH-002
```

**Option B: Single device**
```bash
python pipeline/kafka_producer.py \
  --devices RTU-001:rooftop_unit \
  --interval 10
```

### 4. View Messages in Kafka UI

Open http://localhost:8080 and navigate to Topics → iot-metrics

### 5. Start Consumer

**Single consumer (for testing):**
```bash
python pipeline/kafka_consumer.py --aggregation-window 60
```

**Horizontal scaling (multiple consumers, same group):**
```bash
# Terminal 1
python pipeline/kafka_consumer.py --group-id iot-group --aggregation-window 60

# Terminal 2 (will auto-balance partitions with Terminal 1)
python pipeline/kafka_consumer.py --group-id iot-group --aggregation-window 60
```

The consumer will:
- Read individual metrics from Kafka
- Aggregate by device using ScyllaDB buffer (60-second windows)
- Generate embeddings with Ollama
- Write raw metrics to ScyllaDB `device_metrics_raw`
- Write aggregated snapshots to ScyllaDB `device_state_snapshots`

## Configuration

### Fleet Config (`fleet_config.json`)

Defines the device fleet:
```json
{
  "devices": [
    {
      "device_id": "RTU-001",
      "device_type": "rooftop_unit",
      "location": "building-A",
      "building_id": "bldg-001"
    }
  ]
}
```

Current fleet: 11 devices (3 RTUs, 2 MAUs, 2 Chillers, 2 Cooling Towers, 2 Compressors)

## Message Format

Each metric is sent as a separate Kafka message:

```json
{
  "device_id": "RTU-001",
  "device_type": "rooftop_unit",
  "timestamp": "2025-11-17T18:00:00Z",
  "metric_name": "supply_air_temp",
  "metric_value": 72.5,
  "unit": "°F",
  "location": "building-A",
  "building_id": "bldg-001"
}
```

**Topic**: `iot-metrics`
**Partitioning**: By `device_id` (keeps device metrics ordered)
**Compression**: LZ4

## Stopping

```bash
docker-compose down
```

## Troubleshooting

**Kafka won't start**:
- Ensure Docker Desktop is running
- Check ports 9092, 2181, 8080 are not in use
- View logs: `docker-compose logs kafka`

**Producer connection errors**:
- Wait 15-20 seconds after starting Kafka
- Check Kafka is healthy: `docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092`

**No messages appearing**:
- Check producer logs for errors
- Verify topic created: http://localhost:8080
- Try manual topic creation: `docker exec kafka kafka-topics --create --topic iot-metrics --bootstrap-server localhost:9092`

## Next Steps

- [x] Create Kafka consumer to aggregate device state
- [x] Build embedding service using Ollama
- [x] Design ScyllaDB schema
- [x] Create ScyllaDB writer
- [x] Build device profiles
- [x] Implement anomaly detection (3 methods)
- [x] Create real-time dashboard
