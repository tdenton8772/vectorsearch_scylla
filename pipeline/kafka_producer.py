#!/usr/bin/env python3
"""
Kafka producer that runs IoT device simulators and streams metrics to Kafka.

Usage:
    python kafka_producer.py --devices RTU-001:rooftop_unit RTU-002:rooftop_unit CH-001:chiller
    python kafka_producer.py --fleet-config fleet.json
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Set
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add parent directory to path for imports
sys.path.insert(0, '/Users/tdenton/Development/reinvent_demo')
from iot_simulator.iot_simulator import DEVICE_TYPES

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


class IoTKafkaProducer:
    """Producer that runs device simulators and streams to Kafka."""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'iot-metrics',
        interval: float = 60.0
    ):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker address
            topic: Topic to publish metrics to
            interval: Tick interval in seconds (time between metric emissions)
        """
        self.topic = topic
        self.interval = interval
        self.devices: Dict[str, object] = {}
        self.anomaly_device_ids: Set[str] = set()
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=5,
            compression_type='gzip',  # gzip is built-in
            linger_ms=10,  # Batch messages for 10ms
            batch_size=16384,  # 16KB batches
        )
        logger.info(f"Kafka producer initialized: {bootstrap_servers}, topic={topic}")
    
    def add_device(
        self,
        device_id: str,
        device_type: str,
        location: str = 'building-A',
        building_id: str = 'bldg-001',
        inject_anomalies: bool = False
    ):
        """Add a device simulator to the producer."""
        if device_type not in DEVICE_TYPES:
            raise ValueError(f"Unknown device type: {device_type}. Valid: {list(DEVICE_TYPES.keys())}")
        
        simulator_class = DEVICE_TYPES[device_type]
        simulator = simulator_class(
            device_id=device_id,
            location=location,
            building_id=building_id
        )
        
        self.devices[device_id] = simulator
        if inject_anomalies:
            self.anomaly_device_ids.add(device_id)
        
        logger.info(f"Added device: {device_id} ({device_type}), anomalies={inject_anomalies}")
    
    def send_metric(self, device_id: str, metric_data: Dict):
        """Send a single metric to Kafka."""
        try:
            # Use device_id as key for partitioning (keeps device metrics ordered)
            future = self.producer.send(
                self.topic,
                key=device_id,
                value=metric_data
            )
            # Non-blocking send, but we can add callback if needed
            future.add_callback(lambda metadata: None)  # Success callback
            future.add_errback(lambda e: logger.error(f"Failed to send metric: {e}"))
        except KafkaError as e:
            logger.error(f"Kafka error sending metric from {device_id}: {e}")
    
    def run(self, duration: float = None):
        """
        Run all device simulators and stream to Kafka.
        
        Args:
            duration: Run duration in seconds (None = run forever)
        """
        if not self.devices:
            logger.error("No devices configured. Add devices before running.")
            return
        
        logger.info(f"Starting producer with {len(self.devices)} devices")
        logger.info(f"Tick interval: {self.interval}s")
        logger.info(f"Devices with anomalies: {self.anomaly_device_ids or 'none'}")
        
        start_time = time.time()
        tick_count = 0
        total_metrics_sent = 0
        
        try:
            while True:
                tick_start = time.time()
                tick_count += 1
                
                # Get current timestamp
                timestamp = datetime.now(timezone.utc).isoformat()
                
                # Tick all devices
                for device_id, simulator in self.devices.items():
                    # Inject anomalies if configured (start anomaly on first tick)
                    if device_id in self.anomaly_device_ids and tick_count == 1:
                        # Just set a flag to enable anomalies - simulator handles the actual anomaly logic
                        simulator.start_anomaly('simulated_anomaly')
                    
                    # Get current metrics
                    metrics = simulator.tick()
                    
                    # Send each metric as separate message
                    for metric_name, metric_value in metrics.items():
                        # Get unit from metric spec
                        unit = simulator.metrics[metric_name].spec.unit
                        
                        message = {
                            'device_id': device_id,
                            'device_type': simulator.device_type,
                            'timestamp': timestamp,
                            'metric_name': metric_name,
                            'metric_value': round(metric_value, 2),
                            'unit': unit,
                            'location': simulator.location,
                            'building_id': simulator.building_id
                        }
                        self.send_metric(device_id, message)
                        total_metrics_sent += 1
                
                # Flush to ensure messages are sent
                self.producer.flush()
                
                # Log progress
                if tick_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = total_metrics_sent / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Tick {tick_count}: {total_metrics_sent} metrics sent "
                        f"({rate:.1f} msg/sec)"
                    )
                
                # Check duration
                if duration and (time.time() - start_time) >= duration:
                    logger.info(f"Duration {duration}s reached, stopping")
                    break
                
                # Sleep until next tick
                elapsed = time.time() - tick_start
                sleep_time = max(0, self.interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            # Final flush and close
            logger.info("Flushing and closing producer...")
            self.producer.flush()
            self.producer.close()
            logger.info(f"Total metrics sent: {total_metrics_sent}")
    
    def close(self):
        """Close the Kafka producer."""
        self.producer.close()


def load_fleet_config(config_path: str) -> List[Dict]:
    """Load fleet configuration from JSON file."""
    with open(config_path) as f:
        config = json.load(f)
    return config.get('devices', [])


def main():
    parser = argparse.ArgumentParser(
        description='Run IoT device simulators and stream to Kafka'
    )
    parser.add_argument(
        '--devices',
        nargs='+',
        help='Device specs in format: DEVICE_ID:DEVICE_TYPE (e.g., RTU-001:rooftop_unit)'
    )
    parser.add_argument(
        '--fleet-config',
        help='JSON file with fleet configuration'
    )
    parser.add_argument(
        '--anomaly-devices',
        nargs='+',
        default=[],
        help='Device IDs that should exhibit anomalies'
    )
    parser.add_argument(
        '--kafka-broker',
        default='localhost:9092',
        help='Kafka broker address (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic',
        default='iot-metrics',
        help='Kafka topic to publish to (default: iot-metrics)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=60.0,
        help='Tick interval in seconds (default: 60.0)'
    )
    parser.add_argument(
        '--duration',
        type=float,
        help='Run duration in seconds (default: infinite)'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.devices and not args.fleet_config:
        parser.error("Must specify either --devices or --fleet-config")
    
    # Create producer
    producer = IoTKafkaProducer(
        bootstrap_servers=args.kafka_broker,
        topic=args.topic,
        interval=args.interval
    )
    
    # Add devices
    if args.fleet_config:
        logger.info(f"Loading fleet config from {args.fleet_config}")
        devices = load_fleet_config(args.fleet_config)
        for device in devices:
            # Check both config file and CLI args for anomaly injection
            inject_anomalies = device.get('inject_anomalies', False) or (device['device_id'] in args.anomaly_devices)
            producer.add_device(
                device_id=device['device_id'],
                device_type=device['device_type'],
                location=device.get('location', 'building-A'),
                building_id=device.get('building_id', 'bldg-001'),
                inject_anomalies=inject_anomalies
            )
    else:
        for device_spec in args.devices:
            device_id, device_type = device_spec.split(':')
            producer.add_device(
                device_id=device_id,
                device_type=device_type,
                inject_anomalies=device_id in args.anomaly_devices
            )
    
    # Run producer
    producer.run(duration=args.duration)


if __name__ == '__main__':
    main()
