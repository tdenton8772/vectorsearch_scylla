#!/usr/bin/env python3
"""
Inject extreme anomalies into RTU-001 by directly manipulating the simulator.
This creates obvious anomalies that will trigger detection.
"""

import sys
sys.path.insert(0, '/Users/tdenton/Development/reinvent_demo')

from iot_simulator.iot_simulator import RooftopUnitSimulator
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timezone

def inject_extreme_anomaly(duration_minutes=5):
    """Send extreme anomalous data for RTU-001"""
    
    # Create simulator
    simulator = RooftopUnitSimulator(
        device_id='RTU-001',
        location='building-A',
        building_id='bldg-001'
    )
    
    # Inject MULTIPLE anomalies at once for extreme behavior
    simulator.start_anomaly('compressor_failure')
    simulator.start_anomaly('sensor_drift')
    simulator.start_anomaly('dirty_filter')
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print(f"🚨 Injecting EXTREME anomalies for RTU-001 for {duration_minutes} minutes")
    print("   - Compressor failure")
    print("   - Sensor drift (+15°F)")
    print("   - Dirty filter (high pressure)")
    
    start_time = time.time()
    tick_count = 0
    
    try:
        while (time.time() - start_time) < (duration_minutes * 60):
            tick_count += 1
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Get anomalous metrics
            metrics = simulator.tick()
            
            # Send to Kafka
            for metric_name, metric_value in metrics.items():
                unit = simulator.metrics[metric_name].spec.unit
                message = {
                    'device_id': 'RTU-001',
                    'device_type': 'rooftopunit',
                    'timestamp': timestamp,
                    'metric_name': metric_name,
                    'metric_value': round(metric_value, 2),
                    'unit': unit,
                    'location': 'building-A',
                    'building_id': 'bldg-001'
                }
                producer.send('iot-metrics', key='RTU-001', value=message)
            
            producer.flush()
            
            if tick_count % 6 == 0:
                print(f"  Tick {tick_count}: Sent anomalous data")
            
            time.sleep(10)  # 10 second interval
            
    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        producer.close()
        print(f"✅ Sent {tick_count} ticks of extreme anomalous data")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Inject extreme anomalies')
    parser.add_argument('--duration', type=int, default=5, help='Duration in minutes (default: 5)')
    args = parser.parse_args()
    
    inject_extreme_anomaly(args.duration)
