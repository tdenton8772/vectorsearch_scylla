#!/usr/bin/env python3
"""
Dash UI for IoT Device Monitoring

Shows latest state of all devices with real-time updates.
"""

import os
import sys
from datetime import datetime, timezone
from typing import Dict, List

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.graph_objs as go
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

# Connect to ScyllaDB
auth_provider = PlainTextAuthProvider(
    username=scylla_username,
    password=scylla_password
)

cluster = Cluster(
    scylla_hosts,
    port=scylla_port,
    auth_provider=auth_provider
)

session = cluster.connect(scylla_keyspace)

# Prepare queries
query_latest_snapshots = session.prepare("""
    SELECT device_id, snapshot_time, device_type, location, building_id, 
           metrics, anomaly_score, is_anomalous
    FROM device_state_snapshots
    PER PARTITION LIMIT 1
""")

# Initialize Dash app
app = dash.Dash(
    __name__,
    update_title=None,  # Disable "Updating..." title
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ]
)

app.title = "IoT Device Monitor"

# App layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("🏭 IoT Device Monitor", style={'margin': '0', 'color': '#2c3e50'}),
        html.P("Real-time HVAC equipment monitoring with ScyllaDB Vector Search", 
               style={'margin': '5px 0', 'color': '#7f8c8d'})
    ], style={
        'padding': '20px',
        'backgroundColor': '#ecf0f1',
        'borderBottom': '3px solid #3498db'
    }),
    
    # Stats bar
    html.Div(id='stats-bar', style={
        'display': 'flex',
        'justifyContent': 'space-around',
        'padding': '20px',
        'backgroundColor': '#ffffff',
        'borderBottom': '1px solid #ddd'
    }),
    
    # Device grid
    html.Div([
        html.H2("📊 Device Status", style={'color': '#2c3e50', 'marginBottom': '20px'}),
        html.Div(id='device-grid')
    ], style={'padding': '20px'}),
    
    # Auto-refresh
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # Update every 5 seconds
        n_intervals=0
    )
], style={
    'fontFamily': 'Arial, sans-serif',
    'backgroundColor': '#f8f9fa',
    'minHeight': '100vh'
})


def get_latest_device_states():
    """Query ScyllaDB for latest state of each device."""
    try:
        # Get today's date for querying
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Query each known device directly
        # Hardcode the devices from our fleet for now
        known_devices = ['RTU-001', 'MAU-001', 'CH-001', 'CT-001', 'AC-001']
        
        devices = []
        for device_id in known_devices:
            try:
                result = session.execute("""
                    SELECT device_id, snapshot_time, device_type, location, building_id,
                           metrics, anomaly_score, is_anomalous
                    FROM device_state_snapshots
                    WHERE device_id = %s AND date = %s
                    ORDER BY snapshot_time DESC
                    LIMIT 1
                """, (device_id, today))
                
                row = result.one()
                if row:
                    devices.append({
                        'device_id': row.device_id,
                        'device_type': row.device_type,
                        'location': row.location,
                        'building_id': row.building_id,
                        'snapshot_time': row.snapshot_time,
                        'metrics': row.metrics,
                        'anomaly_score': row.anomaly_score or 0.0,
                        'is_anomalous': row.is_anomalous or False
                    })
            except Exception as e:
                # Skip devices that don't have data yet
                continue
        
        return devices
    except Exception as e:
        print(f"Error querying devices: {e}")
        import traceback
        traceback.print_exc()
        return []


def format_metric_value(value):
    """Format metric value for display."""
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def create_device_card(device: Dict):
    """Create a card component for a device."""
    # Determine status color
    if device['is_anomalous']:
        status_color = '#e74c3c'  # Red
        status_text = '⚠️ Anomalous'
    else:
        status_color = '#27ae60'  # Green
        status_text = '✓ Normal'
    
    # Time since last update
    now = datetime.now(timezone.utc)
    snapshot_time = device['snapshot_time']
    if snapshot_time.tzinfo is None:
        snapshot_time = snapshot_time.replace(tzinfo=timezone.utc)
    time_diff = (now - snapshot_time).total_seconds()
    
    if time_diff < 60:
        last_update = f"{int(time_diff)}s ago"
    elif time_diff < 3600:
        last_update = f"{int(time_diff / 60)}m ago"
    else:
        last_update = f"{int(time_diff / 3600)}h ago"
    
    # Get key metrics based on device type
    metrics = device['metrics']
    key_metrics = []
    
    # Show first 6 most important metrics
    important_metrics = [
        'supply_air_temp', 'return_air_temp', 'outdoor_air_temp',
        'fan_speed', 'power_consumption', 'compressor_status',
        'discharge_pressure', 'motor_current', 'chilled_water_supply_temp',
        'inlet_water_temp', 'outlet_water_temp'
    ]
    
    for metric_name in important_metrics:
        if metric_name in metrics:
            key_metrics.append((metric_name, metrics[metric_name]))
            if len(key_metrics) >= 6:
                break
    
    # If we don't have enough, add others
    if len(key_metrics) < 6:
        for metric_name, value in metrics.items():
            if metric_name not in [m[0] for m in key_metrics]:
                key_metrics.append((metric_name, value))
                if len(key_metrics) >= 6:
                    break
    
    return html.Div([
        # Card header
        html.Div([
            html.Div([
                html.H3(device['device_id'], style={
                    'margin': '0',
                    'fontSize': '18px',
                    'color': '#2c3e50'
                }),
                html.P(device['device_type'].replace('_', ' ').title(), style={
                    'margin': '2px 0',
                    'fontSize': '12px',
                    'color': '#7f8c8d'
                })
            ]),
            html.Div([
                html.Span(status_text, style={
                    'padding': '4px 12px',
                    'borderRadius': '12px',
                    'backgroundColor': status_color,
                    'color': 'white',
                    'fontSize': '12px',
                    'fontWeight': 'bold'
                })
            ])
        ], style={
            'display': 'flex',
            'justifyContent': 'space-between',
            'alignItems': 'center',
            'marginBottom': '15px'
        }),
        
        # Location info
        html.Div([
            html.Span('📍 ', style={'marginRight': '5px'}),
            html.Span(f"{device['location']} ({device['building_id']})", style={
                'fontSize': '13px',
                'color': '#7f8c8d'
            })
        ], style={'marginBottom': '10px'}),
        
        # Metrics
        html.Div([
            html.Div([
                html.Div(metric_name.replace('_', ' ').title(), style={
                    'fontSize': '11px',
                    'color': '#95a5a6',
                    'marginBottom': '2px'
                }),
                html.Div(format_metric_value(value), style={
                    'fontSize': '16px',
                    'fontWeight': 'bold',
                    'color': '#2c3e50'
                })
            ], style={
                'padding': '8px',
                'backgroundColor': '#f8f9fa',
                'borderRadius': '4px',
                'marginBottom': '5px'
            }) for metric_name, value in key_metrics
        ]),
        
        # Footer
        html.Div([
            html.Span(f"🕒 {last_update}", style={
                'fontSize': '11px',
                'color': '#95a5a6'
            }),
            html.Span(f"Metrics: {len(metrics)}", style={
                'fontSize': '11px',
                'color': '#95a5a6',
                'marginLeft': '15px'
            })
        ], style={
            'marginTop': '15px',
            'paddingTop': '10px',
            'borderTop': '1px solid #ecf0f1'
        })
    ], style={
        'backgroundColor': 'white',
        'padding': '20px',
        'borderRadius': '8px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
        'border': f'2px solid {status_color}' if device['is_anomalous'] else '1px solid #ddd'
    })


@app.callback(
    Output('stats-bar', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_stats(n):
    """Update statistics bar."""
    devices = get_latest_device_states()
    
    total_devices = len(devices)
    online_devices = total_devices  # All devices with recent snapshots
    anomalous_devices = sum(1 for d in devices if d['is_anomalous'])
    
    # Get total snapshots count
    try:
        total_snapshots = session.execute(
            "SELECT COUNT(*) FROM device_state_snapshots"
        ).one()[0]
    except:
        total_snapshots = 0
    
    stats = [
        {'label': 'Total Devices', 'value': total_devices, 'icon': '📱', 'color': '#3498db'},
        {'label': 'Online', 'value': online_devices, 'icon': '✓', 'color': '#27ae60'},
        {'label': 'Anomalies', 'value': anomalous_devices, 'icon': '⚠️', 'color': '#e74c3c'},
        {'label': 'Snapshots', 'value': total_snapshots, 'icon': '📊', 'color': '#9b59b6'}
    ]
    
    return [
        html.Div([
            html.Div([
                html.Span(stat['icon'], style={'fontSize': '24px', 'marginRight': '10px'}),
                html.Div([
                    html.Div(stat['value'], style={
                        'fontSize': '28px',
                        'fontWeight': 'bold',
                        'color': stat['color']
                    }),
                    html.Div(stat['label'], style={
                        'fontSize': '12px',
                        'color': '#7f8c8d'
                    })
                ])
            ], style={'display': 'flex', 'alignItems': 'center'})
        ], style={
            'padding': '15px',
            'backgroundColor': '#f8f9fa',
            'borderRadius': '8px',
            'minWidth': '150px'
        }) for stat in stats
    ]


@app.callback(
    Output('device-grid', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_devices(n):
    """Update device grid."""
    devices = get_latest_device_states()
    
    if not devices:
        return html.Div([
            html.P("No devices found. Start the producer and consumer to see data.", style={
                'textAlign': 'center',
                'padding': '40px',
                'color': '#7f8c8d'
            })
        ])
    
    # Create grid of device cards
    return html.Div([
        create_device_card(device) for device in sorted(devices, key=lambda d: d['device_id'])
    ], style={
        'display': 'grid',
        'gridTemplateColumns': 'repeat(auto-fill, minmax(320px, 1fr))',
        'gap': '20px'
    })


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Starting IoT Device Monitor Dashboard")
    print("=" * 60)
    print(f"\nConnected to ScyllaDB: {scylla_hosts[0]}")
    print(f"Keyspace: {scylla_keyspace}")
    print(f"\n📊 Dashboard URL: http://127.0.0.1:8050")
    print("\nPress Ctrl+C to stop\n")
    
    app.run_server(debug=True, host='0.0.0.0', port=8050)
