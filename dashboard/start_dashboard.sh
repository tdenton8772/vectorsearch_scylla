#!/bin/bash
# Safely start dashboard - kills existing instances first

cd "$(dirname "$0")/.."

# Kill any existing dashboard processes
pkill -9 -f "dashboard/app.py" 2>/dev/null
sleep 1

# Verify port 8050 is free
if lsof -ti:8050 > /dev/null 2>&1; then
    echo "❌ Port 8050 is still in use. Killing process..."
    kill -9 $(lsof -ti:8050) 2>/dev/null
    sleep 1
fi

# Start dashboard
echo "🚀 Starting dashboard..."
python dashboard/app.py > logs/dashboard.log 2>&1 &
DASH_PID=$!

sleep 2

# Verify it started
if ps -p $DASH_PID > /dev/null 2>&1; then
    echo "✅ Dashboard started successfully (PID: $DASH_PID)"
    echo "📊 URL: http://localhost:8050"
else
    echo "❌ Dashboard failed to start. Check logs/dashboard.log"
    exit 1
fi
