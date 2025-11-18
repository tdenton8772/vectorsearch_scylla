#!/bin/bash
# Quick start script for local Kafka pipeline

set -e

echo "🚀 Starting local IoT demo pipeline..."
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

# Start Kafka infrastructure
echo "📦 Starting Kafka, Zookeeper, and Redis..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 15

# Check Kafka health
echo "🔍 Checking Kafka health..."
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Kafka is ready"
else
    echo "❌ Kafka failed to start"
    exit 1
fi

echo ""
echo "✅ Infrastructure is ready!"
echo ""
echo "🌐 Services:"
echo "  - Kafka broker: localhost:9092"
echo "  - Kafka UI: http://localhost:8080"
echo "  - Redis: localhost:6379"
echo ""
echo "📝 Next steps:"
echo "  1. Start producer: python pipeline/kafka_producer.py --fleet-config pipeline/fleet_config.json --interval 10"
echo "  2. View messages in Kafka UI: http://localhost:8080"
echo "  3. Start consumer: python pipeline/kafka_consumer.py"
echo ""
echo "To stop: docker-compose down"
