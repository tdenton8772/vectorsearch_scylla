#!/usr/bin/env python3
"""
Rules-Based Anomaly Detection.

Uses hard-coded thresholds and business rules to detect anomalies.
This is the traditional approach - fast, deterministic, and easy to explain.
"""

import argparse
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

# ScyllaDB connection
scylla_hosts = os.getenv('SCYLLA_HOSTS').split(',')
scylla_port = int(os.getenv('SCYLLA_PORT', '19042'))
scylla_username = os.getenv('SCYLLA_USERNAME')
scylla_password = os.getenv('SCYLLA_PASSWORD')
scylla_keyspace = os.getenv('SCYLLA_KEYSPACE', 'iot_monitoring')


# Device-specific rules
DEVICE_RULES = {
    'rooftop_unit': {
        'supply_air_temp': {'min': 50, 'max': 90, 'critical': True},
        'return_air_temp': {'min': 60, 'max': 85},
        'outdoor_air_temp': {'min': -20, 'max': 120},
        'power_consumption': {'min': 0, 'max': 50, 'critical': True},
        'compressor_status': {'min': 0, 'max': 1},
        'fan_speed': {'min': 0, 'max': 1200},
        'fan_current': {'min': 0, 'max': 30},
    },
    'makeup_air_unit': {
        'supply_air_temp': {'min': 50, 'max': 90},
        'return_air_temp': {'min': 60, 'max': 85},
        'outdoor_air_temp': {'min': -20, 'max': 120},
        'power_consumption': {'min': 0, 'max': 45, 'critical': True},
        'fan_speed': {'min': 0, 'max': 1200},
    },
    'chiller': {
        'chilled_water_supply_temp': {'min': 35, 'max': 55, 'critical': True},
        'chilled_water_return_temp': {'min': 45, 'max': 65},
        'condenser_water_temp': {'min': 60, 'max': 100},
        'power_consumption': {'min': 0, 'max': 200, 'critical': True},
        'capacity_percentage': {'min': 0, 'max': 100},
        'refrigerant_pressure': {'min': 0, 'max': 200},
    },
    'cooling_tower': {
        'inlet_water_temp': {'min': 60, 'max': 110},
        'outlet_water_temp': {'min': 50, 'max': 90, 'critical': True},
        'ambient_temp': {'min': -20, 'max': 120},
        'water_flow_rate': {'min': 0, 'max': 2000, 'critical': True},
        'power_consumption': {'min': 0, 'max': 30},
    },
    'air_compressor': {
        'discharge_pressure': {'min': 80, 'max': 150, 'critical': True},
        'discharge_temp': {'min': 100, 'max': 250, 'critical': True},
        'motor_current': {'min': 0, 'max': 100},
        'power_consumption': {'min': 0, 'max': 150, 'critical': True},
        'tank_pressure': {'min': 80, 'max': 150},
    }
}

# Cross-metric rules (relationship constraints)
RELATIONSHIP_RULES = {
    'rooftop_unit': [
        {
            'name': 'compressor_power_correlation',
            'condition': lambda m: m.get('compressor_status', 0) == 1 and m.get('power_consumption', 0) < 5,
            'message': 'Compressor on but power consumption too low'
        },
        {
            'name': 'temp_differential',
            'condition': lambda m: abs(m.get('supply_air_temp', 70) - m.get('return_air_temp', 70)) > 25,
            'message': 'Abnormal temperature differential between supply and return air'
        }
    ],
    'chiller': [
        {
            'name': 'delta_t_check',
            'condition': lambda m: m.get('chilled_water_delta_t', 10) < 3,
            'message': 'Delta T too low - possible flow issues',
            'critical': True
        }
    ]
}


def connect_scylla():
    """Connect to ScyllaDB."""
    auth_provider = PlainTextAuthProvider(
        username=scylla_username,
        password=scylla_password
    )
    
    cluster = Cluster(
        scylla_hosts,
        port=scylla_port,
        auth_provider=auth_provider
    )
    
    return cluster.connect(scylla_keyspace)


def get_recent_snapshots(session, device_id: str, hours_back: int = 1):
    """Get recent snapshots for a device."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)
    
    snapshots = []
    
    current_date = end_time.strftime('%Y-%m-%d')
    query = """
        SELECT device_id, device_type, snapshot_time, metrics, is_anomalous
        FROM device_state_snapshots
        WHERE device_id = %s AND date = %s
        AND snapshot_time >= %s
    """
    
    result = session.execute(query, (device_id, current_date, start_time))
    snapshots.extend(result)
    
    if start_time.date() < end_time.date():
        prev_date = start_time.strftime('%Y-%m-%d')
        result = session.execute(query, (device_id, prev_date, start_time))
        snapshots.extend(result)
    
    return snapshots


def check_threshold_violations(metrics: Dict, device_type: str) -> List[Dict]:
    """Check for threshold violations based on device type."""
    violations = []
    
    rules = DEVICE_RULES.get(device_type, {})
    
    for metric_name, value in metrics.items():
        if metric_name in rules:
            rule = rules[metric_name]
            min_val = rule.get('min')
            max_val = rule.get('max')
            is_critical = rule.get('critical', False)
            
            if min_val is not None and value < min_val:
                violations.append({
                    'metric': metric_name,
                    'value': value,
                    'rule': f'< {min_val}',
                    'severity': 'CRITICAL' if is_critical else 'WARNING',
                    'message': f'{metric_name} below minimum: {value:.2f} < {min_val}'
                })
            
            if max_val is not None and value > max_val:
                violations.append({
                    'metric': metric_name,
                    'value': value,
                    'rule': f'> {max_val}',
                    'severity': 'CRITICAL' if is_critical else 'WARNING',
                    'message': f'{metric_name} above maximum: {value:.2f} > {max_val}'
                })
    
    return violations


def check_relationship_rules(metrics: Dict, device_type: str) -> List[Dict]:
    """Check cross-metric relationship rules."""
    violations = []
    
    rules = RELATIONSHIP_RULES.get(device_type, [])
    
    for rule in rules:
        try:
            if rule['condition'](metrics):
                violations.append({
                    'rule_name': rule['name'],
                    'severity': 'CRITICAL' if rule.get('critical') else 'WARNING',
                    'message': rule['message']
                })
        except KeyError:
            # Missing metric, skip rule
            pass
    
    return violations


def record_rule_anomaly(session, device_id: str, snapshot_time: datetime,
                        threshold_violations: List[Dict],
                        relationship_violations: List[Dict]):
    """Record a rules-based anomaly."""
    date_str = snapshot_time.strftime('%Y-%m-%d')
    
    # Compute anomaly score based on violations
    critical_count = sum(1 for v in threshold_violations if v['severity'] == 'CRITICAL')
    critical_count += sum(1 for v in relationship_violations if v['severity'] == 'CRITICAL')
    warning_count = len(threshold_violations) + len(relationship_violations) - critical_count
    
    anomaly_score = min(1.0, (critical_count * 0.3 + warning_count * 0.1))
    
    # Prepare metrics snapshot
    metrics_snapshot = {
        'detection_method': 'rules_based',
        'threshold_violations': str(len(threshold_violations)),
        'relationship_violations': str(len(relationship_violations)),
        'critical_count': str(critical_count),
        'warning_count': str(warning_count)
    }
    
    # Add violation details
    for i, v in enumerate(threshold_violations[:3]):  # First 3 only
        metrics_snapshot[f'violation_{i}'] = f"{v['metric']}: {v['message']}"
    
    # Insert into anomaly_events
    insert_event = """
        INSERT INTO anomaly_events
        (device_id, detected_at, anomaly_score, anomaly_type, metrics_snapshot)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    session.execute(
        insert_event,
        (device_id, snapshot_time, anomaly_score, 'rules_based', metrics_snapshot)
    )
    
    # Update device statistics
    session.execute(
        "UPDATE device_statistics SET anomalies_detected = anomalies_detected + 1 WHERE device_id = %s",
        (device_id,)
    )
    
    # Mark snapshot as anomalous
    update_snapshot = """
        UPDATE device_state_snapshots
        SET is_anomalous = true, anomaly_score = %s
        WHERE device_id = %s AND date = %s AND snapshot_time = %s
    """
    
    session.execute(
        update_snapshot,
        (anomaly_score, device_id, date_str, snapshot_time)
    )


def analyze_snapshot_rules(
    device_id: str,
    device_type: str,
    snapshot,
    record_events: bool = True
) -> Dict:
    """Analyze a snapshot using rules-based detection."""
    
    # Check threshold violations
    threshold_violations = check_threshold_violations(snapshot.metrics, device_type)
    
    # Check relationship rules
    relationship_violations = check_relationship_rules(snapshot.metrics, device_type)
    
    # Determine if anomalous
    is_anomalous = len(threshold_violations) > 0 or len(relationship_violations) > 0
    
    result = {
        'device_id': device_id,
        'snapshot_time': snapshot.snapshot_time,
        'threshold_violations': threshold_violations,
        'relationship_violations': relationship_violations,
        'is_anomalous': is_anomalous,
        'detection_method': 'rules_based'
    }
    
    return result


def detect_anomalies_for_device(
    session,
    device_id: str,
    hours_back: int = 1,
    record_events: bool = True
) -> list:
    """Detect anomalies for a single device using rules."""
    
    print(f"\n🔍 Analyzing {device_id} (Rules-Based)...")
    
    # Get recent snapshots
    snapshots = get_recent_snapshots(session, device_id, hours_back=hours_back)
    
    if not snapshots:
        print(f"  ℹ️  No recent snapshots found")
        return []
    
    device_type = snapshots[0].device_type
    print(f"  ✓ Device type: {device_type}")
    print(f"  ✓ Analyzing {len(snapshots)} snapshot(s) with {len(DEVICE_RULES.get(device_type, {}))} rules...")
    
    # Analyze each snapshot
    results = []
    anomalies_found = 0
    
    for snapshot in snapshots:
        result = analyze_snapshot_rules(
            device_id,
            device_type,
            snapshot,
            record_events=record_events
        )
        
        results.append(result)
        
        if result['is_anomalous']:
            anomalies_found += 1
            
            # Record anomaly event
            if record_events and not snapshot.is_anomalous:
                record_rule_anomaly(
                    session,
                    device_id,
                    snapshot.snapshot_time,
                    result['threshold_violations'],
                    result['relationship_violations']
                )
    
    # Print summary
    if anomalies_found > 0:
        print(f"  🚨 {anomalies_found} anomal{'y' if anomalies_found == 1 else 'ies'} detected!")
        for result in results:
            if result['is_anomalous']:
                critical = sum(1 for v in result['threshold_violations'] if v['severity'] == 'CRITICAL')
                critical += sum(1 for v in result['relationship_violations'] if v['severity'] == 'CRITICAL')
                
                print(f"      {result['snapshot_time']}:")
                
                # Show violations
                for v in result['threshold_violations'][:2]:
                    print(f"         [{v['severity']}] {v['message']}")
                
                for v in result['relationship_violations'][:2]:
                    print(f"         [{v['severity']}] {v['message']}")
    else:
        print(f"  ✅ All snapshots pass rules validation")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Detect anomalies using rules-based thresholds'
    )
    parser.add_argument(
        '--devices',
        nargs='+',
        default=['RTU-001', 'MAU-001', 'CH-001', 'CT-001', 'AC-001'],
        help='Device IDs to analyze (default: all known devices)'
    )
    parser.add_argument(
        '--hours-back',
        type=int,
        default=1,
        help='Hours of recent snapshots to analyze (default: 1)'
    )
    parser.add_argument(
        '--no-record',
        action='store_true',
        help='Do not record anomalies in database (dry run)'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuously, checking every 30 seconds'
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🔍 Rules-Based Anomaly Detection")
    print("=" * 70)
    print(f"\nDevices: {', '.join(args.devices)}")
    print(f"Time window: {args.hours_back} hour(s)")
    print(f"Record events: {not args.no_record}")
    print(f"Method: Hard thresholds + relationship rules")
    
    # Connect to ScyllaDB
    session = connect_scylla()
    print("\n✅ Connected to ScyllaDB")
    
    def run_detection():
        all_results = []
        total_anomalies = 0
        
        for device_id in args.devices:
            results = detect_anomalies_for_device(
                session,
                device_id,
                hours_back=args.hours_back,
                record_events=not args.no_record
            )
            all_results.extend(results)
            total_anomalies += sum(1 for r in results if r['is_anomalous'])
        
        # Summary
        print("\n" + "=" * 70)
        print(f"Summary: {total_anomalies} anomal{'y' if total_anomalies == 1 else 'ies'} "
              f"out of {len(all_results)} snapshots")
        print(f"Detection method: Rules-based (thresholds + relationships)")
        
        return total_anomalies
    
    if args.continuous:
        import time
        print("\n🔄 Running continuously (Ctrl+C to stop)...")
        try:
            while True:
                run_detection()
                print(f"\n💤 Sleeping 30 seconds...\n")
                time.sleep(30)
        except KeyboardInterrupt:
            print("\n\n✋ Stopped by user")
    else:
        run_detection()
    
    session.cluster.shutdown()


if __name__ == '__main__':
    main()
