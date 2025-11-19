#!/bin/bash
# Check status of all pipeline services

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "🔍 IoT Pipeline Status Check"
echo "============================\n"

# Check Docker services
echo "🐳 Docker Services:"
if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|zookeeper" > /dev/null 2>&1; then
    docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|zookeeper|kafka-ui" | sed 's/^/   /'
else
    echo "   ❌ No Docker services running"
fi

# Check Python processes
echo "\n🐍 Python Services:"
for service in "kafka_producer.py" "kafka_consumer.py" "detect_anomalies_vector_search.py" "dashboard/app.py"; do
    if ps aux | grep -v grep | grep "$service" > /dev/null; then
        pid=$(ps aux | grep -v grep | grep "$service" | awk '{print $2}' | head -1)
        echo "   ✅ $(basename $service .py) (PID: $pid)"
    else
        echo "   ❌ $(basename $service .py) not running"
    fi
done

# Check ScyllaDB connectivity
echo "\n💾 ScyllaDB:"
if python -c "
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv
load_dotenv()
try:
    auth = PlainTextAuthProvider(username=os.getenv('SCYLLA_USERNAME'), password=os.getenv('SCYLLA_PASSWORD'))
    cluster = Cluster(os.getenv('SCYLLA_HOSTS').split(','), port=int(os.getenv('SCYLLA_PORT', '19042')), auth_provider=auth)
    session = cluster.connect('iot_monitoring')
    count = session.execute('SELECT COUNT(*) FROM device_state_snapshots').one()[0]
    print(f'   ✅ Connected - {count} snapshots')
    cluster.shutdown()
except Exception as e:
    print(f'   ❌ Connection failed: {e}')
" 2>&1; then
    :
else
    echo "   ❌ Cannot check ScyllaDB status"
fi

# Check recent logs for errors
echo "\n📝 Recent Log Status:"
if [ -d "logs" ]; then
    for log in logs/*.log; do
        if [ -f "$log" ]; then
            name=$(basename "$log" .log)
            errors=$(tail -50 "$log" 2>/dev/null | grep -i "error\|exception\|failed" | wc -l | tr -d ' ')
            if [ "$errors" -gt 0 ]; then
                echo "   ⚠️  $name: $errors error(s) in last 50 lines"
            else
                echo "   ✅ $name: no recent errors"
            fi
        fi
    done
else
    echo "   ℹ️  No logs directory found"
fi

echo "\n🌐 Service URLs:"
echo "   Dashboard:  http://localhost:8050"
echo "   Kafka UI:   http://localhost:8080"
