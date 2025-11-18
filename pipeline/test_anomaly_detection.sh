#!/bin/bash
# Test script for anomaly detection system

set -e

echo "========================================"
echo "Anomaly Detection System Test"
echo "========================================"
echo ""

# Check if profiles exist
echo "1. Verifying device profiles exist..."
python3 -c "
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
result = session.execute('SELECT COUNT(*) as count FROM device_profiles')
count = result.one().count
print(f'✓ Found {count} device profiles')
if count == 0:
    print('ERROR: No profiles found. Run build_profiles.py first')
    exit(1)
cluster.shutdown()
"

echo ""
echo "2. Starting anomalous device producer..."
echo "   (Running RTU-001 with anomalies for 2 minutes)"
echo ""

# Start producer with anomaly injection in background
python pipeline/kafka_producer.py \
    --devices RTU-001:rooftop_unit \
    --anomaly-devices RTU-001 \
    --interval 10 \
    --duration 120 &

PRODUCER_PID=$!
echo "   Producer PID: $PRODUCER_PID"

# Wait for some snapshots to be created
echo ""
echo "3. Waiting 45 seconds for snapshots to be generated..."
sleep 45

# Run anomaly detection
echo ""
echo "4. Running anomaly detection..."
python pipeline/detect_anomalies.py --devices RTU-001

# Wait for test to complete
echo ""
echo "5. Waiting for producer to finish..."
wait $PRODUCER_PID

echo ""
echo "6. Final anomaly check..."
python pipeline/detect_anomalies.py --devices RTU-001 --no-record

echo ""
echo "========================================"
echo "Test Complete!"
echo "========================================"
echo ""
echo "Check anomaly_events table for recorded anomalies:"
echo "  SELECT device_id, detected_at, anomaly_score FROM anomaly_events WHERE device_id = 'RTU-001';"
