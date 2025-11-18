#!/bin/bash
# Test fleet of IoT devices with one anomalous device

echo "Testing IoT Device Fleet Simulation"
echo "==================================="
echo ""

# Start 3 normal devices in background
echo "Starting 3 normal RTU devices..."
python iot_simulator.py --device-type rooftop_unit --device-id rtu-001 \
    --interval 5 --no-anomalies > /tmp/rtu-001.json 2>&1 &
PID1=$!

python iot_simulator.py --device-type rooftop_unit --device-id rtu-002 \
    --interval 5 --no-anomalies > /tmp/rtu-002.json 2>&1 &
PID2=$!

python iot_simulator.py --device-type rooftop_unit --device-id rtu-003 \
    --interval 5 --no-anomalies > /tmp/rtu-003.json 2>&1 &
PID3=$!

# Start 1 device with anomalies (targeted)
echo "Starting 1 anomalous RTU device (rtu-999)..."
python iot_simulator.py --device-type rooftop_unit --device-id rtu-999 \
    --interval 5 --anomaly-rate 0.3 --anomaly-device-ids rtu-999 \
    > /tmp/rtu-999.json 2>&1 &
PID4=$!

echo ""
echo "Devices running:"
echo "  rtu-001 (normal) - PID $PID1"
echo "  rtu-002 (normal) - PID $PID2"
echo "  rtu-003 (normal) - PID $PID3"
echo "  rtu-999 (anomalous) - PID $PID4"
echo ""
echo "Output files:"
echo "  /tmp/rtu-001.json"
echo "  /tmp/rtu-002.json"
echo "  /tmp/rtu-003.json"
echo "  /tmp/rtu-999.json"
echo ""
echo "Let run for 30 seconds..."
sleep 30

echo ""
echo "Stopping devices..."
kill $PID1 $PID2 $PID3 $PID4 2>/dev/null
wait

echo ""
echo "Sample normal device output (rtu-001):"
head -3 /tmp/rtu-001.json | jq -c '{device_id, metric_name, metric_value}'

echo ""
echo "Sample anomalous device output (rtu-999):"
head -3 /tmp/rtu-999.json | jq -c '{device_id, metric_name, metric_value}'

echo ""
echo "Done! Check /tmp/rtu-*.json for full output"
