# Reset Pipeline to a Clean 5-minute Window

Use these steps to purge Kafka, clear Scylla tables, collect 5 minutes of fresh data, build profiles, and start ANN detection.

## Steps

1. Stop running processes

```bash
pkill -f kafka_consumer.py || true
pkill -f kafka_producer.py || true
pkill -f path3_vector_search.py || true
```

2. Purge Kafka topic(s)

```bash
python pipeline/kafka_reset_topics.py --bootstrap localhost:9092 iot-metrics
```

3. Clear Scylla tables (non-interactive)

```bash
python pipeline/clear_all_data.py --yes
```

4. Start producer and consumer (consumer will look back at most 5 min only if no committed offsets)

```bash
nohup python pipeline/kafka_producer.py --fleet-config pipeline/fleet_config.json --interval 10 > logs/kafka_producer.log 2>&1 &
nohup python pipeline/kafka_consumer.py --aggregation-window 60 --lookback-minutes 5 > logs/kafka_consumer.log 2>&1 &
```

5. Wait 5 minutes to accumulate normal data, then build profiles

```bash
sleep 300
python pipeline/build_profiles.py --days-back 1 --min-snapshots 5
```

6. Start Path 3 ANN detector (watermark persists; first run looks back 5 minutes)

```bash
nohup python -u pipeline/path3_vector_search.py > logs/path3_detector.log 2>&1 &
```
