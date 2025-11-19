#!/bin/bash
# Start all pipeline services
#
# This script starts:
# 1. Kafka infrastructure (docker-compose)
# 2. Device simulators (producer)
# 3. Metric aggregator (consumer)
# 4. Continuous anomaly detection
# 5. Dashboard

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "🚀 Starting IoT Anomaly Detection Pipeline"
echo "==========================================\n"

# Create logs directory
mkdir -p logs

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

# Start Kafka infrastructure
echo "📦 Starting Kafka infrastructure..."
cd pipeline
docker-compose up -d
cd ..

# Wait for Kafka
echo "⏳ Waiting for Kafka to be ready..."
sleep 15

# Check Kafka health
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Kafka is ready"
else
    echo "❌ Kafka failed to start"
    exit 1
fi

# Start Producer
echo "\n📡 Starting device simulators..."
nohup python pipeline/kafka_producer.py \
    --fleet-config pipeline/fleet_config.json \
    --interval 10 \
    > logs/producer.log 2>&1 &
PRODUCER_PID=$!
echo "   PID: $PRODUCER_PID"

# Start Consumer
echo "🔄 Starting metric aggregator..."
nohup python pipeline/kafka_consumer.py \
    --aggregation-window 60 \
    > logs/consumer.log 2>&1 &
CONSUMER_PID=$!
echo "   PID: $CONSUMER_PID"

# Wait for initial data collection
echo "\n⏳ Waiting 90 seconds for initial data collection..."
sleep 90

# Build device profiles
echo "📊 Building device profiles..."
python pipeline/build_profiles.py

# Start continuous anomaly detection
echo "🔍 Starting continuous anomaly detection..."
nohup python -u pipeline/detect_anomalies_vector_search.py \
    --continuous \
    --only-new \
    > logs/anomaly_detector.log 2>&1 &
DETECTOR_PID=$!
echo "   PID: $DETECTOR_PID"

# Start Dashboard
echo "📊 Starting dashboard..."
nohup python dashboard/app.py \
    > logs/dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo "   PID: $DASHBOARD_PID"

# Save PIDs for easy shutdown
cat > logs/pids.txt <<EOF
$PRODUCER_PID
$CONSUMER_PID
$DETECTOR_PID
$DASHBOARD_PID
EOF

echo "\n✅ All services started!\n"
echo "📊 Dashboard: http://localhost:8050"
echo "🌐 Kafka UI:  http://localhost:8080\n"
echo "📋 Process IDs:"
echo "   Producer:  $PRODUCER_PID"
echo "   Consumer:  $CONSUMER_PID"
echo "   Detector:  $DETECTOR_PID"
echo "   Dashboard: $DASHBOARD_PID\n"
echo "📝 Logs:"
echo "   tail -f logs/producer.log"
echo "   tail -f logs/consumer.log"
echo "   tail -f logs/anomaly_detector.log"
echo "   tail -f logs/dashboard.log\n"
echo "⛔ To stop all services:"
echo "   ./pipeline/stop_pipeline.sh"
