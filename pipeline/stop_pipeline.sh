#!/bin/bash
# Stop all pipeline services

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "🛑 Stopping IoT Anomaly Detection Pipeline"
echo "=========================================\n"

# Read PIDs if file exists
if [ -f logs/pids.txt ]; then
    echo "📋 Stopping services from logs/pids.txt..."
    while read pid; do
        if ps -p $pid > /dev/null 2>&1; then
            echo "   Stopping PID $pid..."
            kill $pid 2>/dev/null || true
        fi
    done < logs/pids.txt
    rm logs/pids.txt
else
    echo "⚠️  No pids.txt found, stopping by process name..."
fi

# Fallback: stop by process name
echo "\n🔍 Ensuring all processes are stopped..."
pkill -f "kafka_producer.py" || true
pkill -f "kafka_consumer.py" || true
pkill -f "detect_anomalies_vector_search.py" || true
pkill -f "dashboard/app.py" || true

# Stop Docker services
echo "🐳 Stopping Docker services..."
cd pipeline
docker-compose down
cd ..

echo "\n✅ All services stopped!"
echo "\n📝 Log files preserved in logs/ directory"
