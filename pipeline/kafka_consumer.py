#!/usr/bin/env python3
"""
Kafka consumer that:
1. Reads individual metrics from Kafka
2. Aggregates metrics by device using ScyllaDB (time windows)
3. Generates embeddings (Ollama text-based)
4. Writes to ScyllaDB:
   - Raw metrics → device_metrics_raw
   - Buffered metrics → metric_aggregation_buffer
   - Aggregated state → device_state_snapshots

Usage:
    python kafka_consumer.py --group-id iot-consumer-1
    
To scale horizontally, run multiple consumers with same group-id:
    python kafka_consumer.py --group-id iot-consumer-group
    python kafka_consumer.py --group-id iot-consumer-group  # Will auto-balance partitions
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel
import ollama
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


class IoTConsumer:
    """
    Kafka consumer that aggregates device metrics and writes to ScyllaDB.
    
    Uses ScyllaDB for stateful aggregation by time window (no Redis needed).
    Supports horizontal scaling via Kafka consumer groups.
    """
    
    def __init__(
        self,
        kafka_brokers: str = 'localhost:9092',
        kafka_topic: str = 'iot-metrics',
        consumer_group: str = 'iot-consumer-group',
        aggregation_window: int = 60,  # seconds
        scylla_hosts: List[str] = None,
        scylla_port: int = 19042,
        scylla_username: str = None,
        scylla_password: str = None,
        scylla_keyspace: str = 'iot_monitoring',
        embedding_method: str = 'ollama_text'
    ):
        """
        Initialize consumer.
        
        Args:
            kafka_brokers: Kafka broker addresses
            kafka_topic: Topic to consume from
            consumer_group: Consumer group ID (same group = auto partition balancing)
            aggregation_window: Time window in seconds for aggregating metrics
            scylla_hosts: ScyllaDB contact points
            scylla_port: ScyllaDB port
            scylla_username: ScyllaDB username
            scylla_password: ScyllaDB password
            scylla_keyspace: ScyllaDB keyspace
            embedding_method: 'ollama_text' or 'tabtransformer'
        """
        self.kafka_topic = kafka_topic
        self.consumer_group = consumer_group
        self.aggregation_window = aggregation_window
        self.embedding_method = embedding_method
        self.scylla_keyspace = scylla_keyspace
        
        # Connect to Kafka
        logger.info(f"Connecting to Kafka: {kafka_brokers}, topic: {kafka_topic}")
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_brokers.split(','),
            group_id=consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',  # Start from latest for new consumers
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            max_poll_records=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        logger.info(f"✅ Kafka consumer initialized (group: {consumer_group})")
        
        # Connect to ScyllaDB
        logger.info(f"Connecting to ScyllaDB: {scylla_hosts}")
        if scylla_username and scylla_password:
            auth_provider = PlainTextAuthProvider(
                username=scylla_username,
                password=scylla_password
            )
            cluster = Cluster(
                scylla_hosts,
                port=scylla_port,
                auth_provider=auth_provider
            )
        else:
            cluster = Cluster(scylla_hosts, port=scylla_port)
        
        self.scylla_session = cluster.connect(scylla_keyspace)
        logger.info(f"✅ ScyllaDB connected (keyspace: {scylla_keyspace})")
        
        # Prepare statements for performance
        self._prepare_statements()
        
        # Track active windows for efficient processing
        self.active_windows = set()  # (device_id, window_start) tuples
        
        # Statistics
        self.stats = {
            'messages_consumed': 0,
            'raw_metrics_written': 0,
            'snapshots_written': 0,
            'errors': 0
        }
    
    def _prepare_statements(self):
        """Prepare CQL statements for better performance."""
        # Insert raw metric
        self.insert_raw_metric = self.scylla_session.prepare("""
            INSERT INTO device_metrics_raw 
            (device_id, date, timestamp, metric_name, metric_value, unit, 
             device_type, location, building_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Insert/update metric in aggregation buffer
        self.upsert_buffer_metric = self.scylla_session.prepare("""
            INSERT INTO metric_aggregation_buffer
            (device_id, window_start, metric_name, metric_value,
             device_type, location, building_id, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Query all metrics for a device window
        self.query_buffer_metrics = self.scylla_session.prepare("""
            SELECT metric_name, metric_value, device_type, location, building_id
            FROM metric_aggregation_buffer
            WHERE device_id = ? AND window_start = ?
        """)
        
        # Delete buffer after processing
        self.delete_buffer_window = self.scylla_session.prepare("""
            DELETE FROM metric_aggregation_buffer
            WHERE device_id = ? AND window_start = ?
        """)
        
        # Insert device state snapshot
        self.insert_snapshot = self.scylla_session.prepare("""
            INSERT INTO device_state_snapshots
            (device_id, date, snapshot_time, device_type, location, building_id,
             metrics, embedding, embedding_method, anomaly_score, is_anomalous)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        logger.info("✅ CQL statements prepared")
    
    def _get_window_start(self, timestamp: datetime) -> datetime:
        """Get the start of the aggregation window for a timestamp."""
        ts_seconds = int(timestamp.timestamp())
        window_start_seconds = (ts_seconds // self.aggregation_window) * self.aggregation_window
        return datetime.fromtimestamp(window_start_seconds, tz=timezone.utc)
    
    def write_raw_metric(self, message: Dict):
        """Write individual metric to device_metrics_raw table."""
        try:
            timestamp = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            date_str = timestamp.strftime('%Y-%m-%d')
            
            self.scylla_session.execute(
                self.insert_raw_metric,
                (
                    message['device_id'],
                    date_str,
                    timestamp,
                    message['metric_name'],
                    message['metric_value'],
                    message['unit'],
                    message['device_type'],
                    message['location'],
                    message['building_id']
                )
            )
            self.stats['raw_metrics_written'] += 1
            
        except Exception as e:
            logger.error(f"Error writing raw metric: {e}")
            self.stats['errors'] += 1
    
    def aggregate_metric(self, message: Dict):
        """Add metric to ScyllaDB aggregation buffer."""
        try:
            timestamp = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            window_start = self._get_window_start(timestamp)
            
            # Track this window as active
            self.active_windows.add((message['device_id'], window_start))
            
            # Upsert metric into buffer (ScyllaDB will overwrite if same key)
            self.scylla_session.execute(
                self.upsert_buffer_metric,
                (
                    message['device_id'],
                    window_start,
                    message['metric_name'],
                    message['metric_value'],
                    message['device_type'],
                    message['location'],
                    message['building_id'],
                    datetime.now(timezone.utc)
                )
            )
            
        except Exception as e:
            logger.error(f"Error aggregating metric: {e}")
            self.stats['errors'] += 1
    
    def generate_embedding_ollama(self, device_state: Dict) -> List[float]:
        """
        Generate embedding from device state using Ollama.
        
        Converts device state to natural language text, then embeds.
        """
        try:
            # Convert device state to natural language
            device_id = device_state['device_id']
            device_type = device_state['device_type']
            metrics = device_state['metrics']
            
            # Build text representation
            text_parts = [f"{device_id} {device_type}"]
            for metric_name, metric_value in sorted(metrics.items()):
                # Format nicely
                if isinstance(metric_value, float):
                    text_parts.append(f"{metric_name} {metric_value:.2f}")
                else:
                    text_parts.append(f"{metric_name} {metric_value}")
            
            text = ": " + ", ".join(text_parts)
            
            # Generate embedding with Ollama
            response = ollama.embeddings(
                model='all-minilm:l6-v2',
                prompt=text
            )
            
            return response['embedding']
            
        except Exception as e:
            logger.error(f"Error generating Ollama embedding: {e}")
            # Return zero vector on error
            return [0.0] * 384
    
    def check_and_write_snapshots(self):
        """
        Check for completed aggregation windows and write snapshots.
        
        For each window, groups metrics by exact timestamp and emits
        one snapshot per unique timestamp (not one per window).
        """
        try:
            now = datetime.now(timezone.utc)
            cutoff_time = now - timedelta(seconds=self.aggregation_window * 2)
            
            # Process windows that are old enough
            windows_to_process = [
                (device_id, window_start)
                for device_id, window_start in self.active_windows
                if window_start < cutoff_time
            ]
            
            for device_id, window_start in windows_to_process:
                try:
                    # Query raw metrics from device_metrics_raw within this window
                    window_end = window_start + timedelta(seconds=self.aggregation_window)
                    date_str = window_start.strftime('%Y-%m-%d')
                    
                    query = """
                        SELECT timestamp, metric_name, metric_value, device_type, location, building_id
                        FROM device_metrics_raw
                        WHERE device_id = %s AND date = %s
                        AND timestamp >= %s AND timestamp < %s
                    """
                    
                    raw_metrics = list(self.scylla_session.execute(
                        query,
                        (device_id, date_str, window_start, window_end)
                    ))
                    
                    if not raw_metrics:
                        self.active_windows.discard((device_id, window_start))
                        continue
                    
                    # Group metrics by exact timestamp
                    from collections import defaultdict
                    timestamp_groups = defaultdict(list)
                    
                    for row in raw_metrics:
                        timestamp_groups[row.timestamp].append(row)
                    
                    # Create one snapshot per unique timestamp
                    for timestamp, metrics_rows in timestamp_groups.items():
                        first_row = metrics_rows[0]
                        device_type = first_row.device_type
                        location = first_row.location
                        building_id = first_row.building_id
                        
                        # Build metrics dict
                        metrics = {r.metric_name: r.metric_value for r in metrics_rows}
                        
                        # Build device state
                        device_state = {
                            'device_id': device_id,
                            'device_type': device_type,
                            'location': location,
                            'building_id': building_id,
                            'snapshot_time': timestamp,
                            'metrics': metrics
                        }
                        
                        # Generate embedding
                        embedding = self.generate_embedding_ollama(device_state)
                        
                        # Write to ScyllaDB
                        self.write_snapshot(device_state, embedding)
                        
                        logger.debug(
                            f"✅ Snapshot: {device_id} @ {timestamp.isoformat()} ({len(metrics)} metrics)"
                        )
                    
                    # Delete from buffer (processed)
                    self.scylla_session.execute(
                        self.delete_buffer_window,
                        (device_id, window_start)
                    )
                    
                    # Remove from active windows
                    self.active_windows.discard((device_id, window_start))
                    
                    logger.info(
                        f"✅ Window processed: {device_id} @ {window_start.isoformat()} "
                        f"({len(timestamp_groups)} snapshots from {len(raw_metrics)} metrics)"
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing window {device_id}@{window_start}: {e}")
                    self.stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"Error checking/writing snapshots: {e}")
            self.stats['errors'] += 1
    
    def write_snapshot(self, device_state: Dict, embedding: List[float]):
        """Write aggregated device state snapshot to ScyllaDB."""
        try:
            snapshot_time = device_state['snapshot_time']
            date_str = snapshot_time.strftime('%Y-%m-%d')
            
            # For now, anomaly detection is simple (placeholder)
            anomaly_score = 0.0
            is_anomalous = False
            
            self.scylla_session.execute(
                self.insert_snapshot,
                (
                    device_state['device_id'],
                    date_str,
                    snapshot_time,
                    device_state['device_type'],
                    device_state['location'],
                    device_state['building_id'],
                    device_state['metrics'],
                    embedding,
                    self.embedding_method,
                    anomaly_score,
                    is_anomalous
                )
            )
            self.stats['snapshots_written'] += 1
            
        except Exception as e:
            logger.error(f"Error writing snapshot: {e}")
            self.stats['errors'] += 1
    
    def run(self):
        """Main consumer loop."""
        logger.info(f"Starting consumer...")
        logger.info(f"  Consumer group: {self.consumer_group}")
        logger.info(f"  Aggregation window: {self.aggregation_window}s")
        logger.info(f"  Embedding method: {self.embedding_method}")
        logger.info(f"  Using ScyllaDB for aggregation (no Redis)")
        
        last_snapshot_check = time.time()
        snapshot_check_interval = self.aggregation_window / 2  # Check twice per window
        
        try:
            for message in self.consumer:
                try:
                    msg_value = message.value
                    
                    # Write raw metric to ScyllaDB
                    self.write_raw_metric(msg_value)
                    
                    # Aggregate in ScyllaDB buffer
                    self.aggregate_metric(msg_value)
                    
                    self.stats['messages_consumed'] += 1
                    
                    # Periodically check for completed windows and write snapshots
                    now = time.time()
                    if now - last_snapshot_check >= snapshot_check_interval:
                        self.check_and_write_snapshots()
                        last_snapshot_check = now
                    
                    # Log progress
                    if self.stats['messages_consumed'] % 100 == 0:
                        logger.info(
                            f"Progress: {self.stats['messages_consumed']} consumed, "
                            f"{self.stats['raw_metrics_written']} raw written, "
                            f"{self.stats['snapshots_written']} snapshots, "
                            f"{self.stats['errors']} errors"
                        )
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.stats['errors'] += 1
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.close()
    
    def close(self):
        """Clean up resources."""
        logger.info("Closing consumer...")
        
        # Final snapshot check
        logger.info("Processing remaining snapshots...")
        self.check_and_write_snapshots()
        
        # Close connections
        self.consumer.close()
        
        logger.info("\n=== Final Statistics ===")
        for key, value in self.stats.items():
            logger.info(f"  {key}: {value}")
        logger.info("Consumer closed")


def main():
    parser = argparse.ArgumentParser(
        description='IoT metrics Kafka consumer with ScyllaDB aggregation'
    )
    parser.add_argument(
        '--kafka-brokers',
        default=os.getenv('KAFKA_BROKERS', 'localhost:9092'),
        help='Kafka broker addresses (default: localhost:9092)'
    )
    parser.add_argument(
        '--kafka-topic',
        default='iot-metrics',
        help='Kafka topic to consume from (default: iot-metrics)'
    )
    parser.add_argument(
        '--group-id',
        default='iot-consumer-group',
        help='Consumer group ID for partition balancing (default: iot-consumer-group)'
    )
    parser.add_argument(
        '--aggregation-window',
        type=int,
        default=60,
        help='Aggregation window in seconds (default: 60)'
    )
    parser.add_argument(
        '--embedding-method',
        choices=['ollama_text', 'tabtransformer'],
        default='ollama_text',
        help='Embedding method (default: ollama_text)'
    )
    
    args = parser.parse_args()
    
    # Load ScyllaDB config from environment
    scylla_hosts = os.getenv('SCYLLA_HOSTS', 'localhost').split(',')
    scylla_port = int(os.getenv('SCYLLA_PORT', '19042'))
    scylla_username = os.getenv('SCYLLA_USERNAME')
    scylla_password = os.getenv('SCYLLA_PASSWORD')
    scylla_keyspace = os.getenv('SCYLLA_KEYSPACE', 'iot_monitoring')
    
    # Create and run consumer
    consumer = IoTConsumer(
        kafka_brokers=args.kafka_brokers,
        kafka_topic=args.kafka_topic,
        consumer_group=args.group_id,
        aggregation_window=args.aggregation_window,
        scylla_hosts=scylla_hosts,
        scylla_port=scylla_port,
        scylla_username=scylla_username,
        scylla_password=scylla_password,
        scylla_keyspace=scylla_keyspace,
        embedding_method=args.embedding_method
    )
    
    consumer.run()


if __name__ == '__main__':
    main()
