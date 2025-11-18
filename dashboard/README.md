# IoT Device Monitor Dashboard

Real-time Dash UI for monitoring HVAC equipment with ScyllaDB Vector Search.

## Features

- **Real-time Updates**: Refreshes every 5 seconds
- **Device Cards**: Shows latest state of each device with key metrics
- **Statistics Bar**: Total devices, online count, anomalies, total snapshots
- **Clean UI**: Responsive grid layout with status indicators

## Quick Start

```bash
# Install dependencies
pip install -r dashboard/requirements.txt

# Start dashboard
python dashboard/app.py
```

Dashboard will be available at: **http://127.0.0.1:8050**

## What You'll See

### Stats Bar (Top)
- 📱 Total Devices
- ✓ Online devices
- ⚠️ Anomalies detected
- 📊 Total snapshots in database

### Device Cards
Each device shows:
- Device ID and type (RTU, MAU, Chiller, etc.)
- Status: ✓ Normal or ⚠️ Anomalous
- Location and building
- 6 key metrics (temperature, fan speed, power, etc.)
- Last update time
- Total metric count

### Status Indicators
- **Green border**: Normal operation
- **Red border**: Anomalous behavior detected

## Data Source

Dashboard queries ScyllaDB `device_state_snapshots` table for the latest aggregated device states.

## Requirements

- ScyllaDB with `iot_monitoring` keyspace
- Running producer and consumer to generate data
- Python 3.11+

## Scaling

The dashboard auto-scales with your device fleet. As you add more devices to the producer, they'll automatically appear in the grid.

## Next Steps

- Add device detail views (click to see all metrics)
- Add time-series charts for metric history
- Add anomaly detection visualization
- Add vector similarity comparison between devices
