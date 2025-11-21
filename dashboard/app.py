#!/usr/bin/env python3
"""
Dash UI for IoT Device Monitoring

Shows latest state of all devices with real-time updates.
"""

import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List

import dash
from dash import dcc, html, dash_table, no_update, ctx
from dash.dependencies import Input, Output, State, ALL
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

    # Device detail section
    html.Div([
        html.H2("🔎 Device Details", style={'color': '#2c3e50', 'margin': '0 0 10px 0'}),
        html.Div([
            html.Div([
                html.Span("Selected: ", style={'color': '#7f8c8d'}),
                html.Strong(id='selected-device-label', children='(click a device card)')
            ]),
            html.Div([
                html.Span("Time Range: ", style={'marginRight': '8px', 'color': '#7f8c8d'}),
                dcc.Dropdown(
                    id='hours-back',
                    options=[
                        {'label': '1 hour', 'value': 1},
                        {'label': '3 hours', 'value': 3},
                        {'label': '6 hours', 'value': 6},
                        {'label': '24 hours', 'value': 24},
                    ],
                    value=6,
                    clearable=False,
                    style={'width': '180px'}
                )
            ], style={'display': 'flex', 'alignItems': 'center', 'gap': '10px'})
        ], style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center', 'marginBottom': '10px'}),
        html.Div(id='device-metric-graphs')
    ], style={'padding': '20px', 'backgroundColor': '#ffffff', 'borderTop': '1px solid #ddd'}),
    
    # URL location for device selection (most reliable)
    dcc.Location(id='url', refresh=False),
    
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
    ], id={'type': 'device-card', 'device_id': device['device_id']}, n_clicks=0, style={
        'backgroundColor': 'white',
        'padding': '20px',
        'borderRadius': '8px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
        'border': f'2px solid {status_color}' if device['is_anomalous'] else '1px solid #ddd',
        'cursor': 'pointer'
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


def get_device_history(device_id: str, hours_back: int):
    """Fetch historical snapshots for a device within the last N hours."""
    try:
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=hours_back)
        # Collect dates to query (today and possibly yesterday)
        dates = sorted({start_time.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d')})
        rows = []
        for date_str in dates:
            # For each date, query snapshots with lower bound when applicable
            if date_str == start_time.strftime('%Y-%m-%d'):
                result = session.execute(
                    """
                    SELECT snapshot_time, device_type, metrics, is_anomalous, anomaly_score
                    FROM device_state_snapshots
                    WHERE device_id = %s AND date = %s AND snapshot_time >= %s
                    ORDER BY snapshot_time ASC
                    """,
                    (device_id, date_str, start_time)
                )
            else:
                result = session.execute(
                    """
                    SELECT snapshot_time, device_type, metrics, is_anomalous, anomaly_score
                    FROM device_state_snapshots
                    WHERE device_id = %s AND date = %s
                    ORDER BY snapshot_time ASC
                    """,
                    (device_id, date_str)
                )
            rows.extend(list(result))
        # Sort by time
        rows.sort(key=lambda r: r.snapshot_time)
        return rows
    except Exception as e:
        print(f"Error fetching history for {device_id}: {e}")
        return []


def get_anomaly_events(device_id: str, hours_back: int):
    """Fetch anomaly events for a device."""
    try:
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=hours_back)
        dates = sorted({start_time.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d')})
        
        anomalies = []
        for date_str in dates:
            # Query by partition key only - can't filter by detected_at without ALLOW FILTERING
            result = session.execute(
                """
                SELECT detected_at, anomaly_score, anomaly_type, metrics_snapshot,
                       path1_rules_triggered, path2_fingerprint_triggered, path3_vector_triggered, detection_details
                FROM anomaly_events
                WHERE device_id = %s AND date = %s
                """,
                (device_id, date_str)
            )
            # Filter in Python for time range
            for row in result:
                # Handle timezone-aware comparison
                detected = row.detected_at
                if detected.tzinfo is None:
                    detected = detected.replace(tzinfo=timezone.utc)
                if detected >= start_time:
                    anomalies.append(row)
        
        return anomalies
    except Exception as e:
        print(f"Error fetching anomalies for {device_id}: {e}")
        return []


def build_metric_graphs(device_id: str, rows: List[Dict], anomalies: List = None):
    """Create a grid of small line charts for each metric with anomaly markers."""
    if not rows:
        return html.Div("No historical data found.", style={'color': '#7f8c8d'})
    
    # Build time series per metric
    times = [r.snapshot_time for r in rows]
    anomaly_flags = [r.is_anomalous for r in rows]
    
    # Collect metrics keys
    metric_keys = set()
    for r in rows:
        metric_keys.update(r.metrics.keys())
    
    # Build anomaly lookup by timestamp
    anomaly_map = {}
    if anomalies:
        print(f"🔍 Building anomaly map for {device_id}: {len(anomalies)} anomalies")
        for anom in anomalies:
            # Build detection path badges
            path_badges = []
            if getattr(anom, 'path1_rules_triggered', False):
                path_badges.append('📊 Rules')
            if getattr(anom, 'path2_fingerprint_triggered', False):
                path_badges.append('🔍 Fingerprint')
            if getattr(anom, 'path3_vector_triggered', False):
                path_badges.append('🎯 Vector Search')
            
            # Use detection_details if available, otherwise build from metrics
            if hasattr(anom, 'detection_details') and anom.detection_details:
                # Format detection_details with proper line breaks
                # Replace common delimiters with <br> for better readability
                reason_text = anom.detection_details.replace('; ', '<br>').replace(' | ', '<br>')
            else:
                # Fallback to old logic
                reasons = []
                if anom.metrics_snapshot:
                    similarity = anom.metrics_snapshot.get('similarity_score')
                    if similarity and similarity < 0.75:
                        reasons.append(f"Low similarity: {similarity:.3f}")
                    
                    outliers = [k.replace('outlier_', '') for k in anom.metrics_snapshot.keys() 
                               if k.startswith('outlier_')]
                    if outliers:
                        reasons.append(f"Outliers: {', '.join(outliers[:3])}")
                
                reason_text = '<br>'.join(reasons) if reasons else 'Anomalous behavior detected'
            
            # Add path badges on separate line before reason
            if path_badges:
                reason_text = f"<b>{' '.join(path_badges)}</b><br>{reason_text}"
            
            # Round detected_at to nearest 10 seconds to match snapshot times
            # Snapshots are taken every 10 seconds, so round to align
            detected_rounded = anom.detected_at.replace(second=(anom.detected_at.second // 10) * 10, microsecond=0)
            anomaly_map[detected_rounded] = {
                'score': anom.anomaly_score,
                'type': anom.anomaly_type,
                'reason': reason_text
            }
            print(f"  Added to map: {detected_rounded} -> score={anom.anomaly_score:.3f}, reason_len={len(reason_text)}")
    
    # Create a small graph per metric
    graphs = []
    for metric in sorted(metric_keys):
        y = [r.metrics.get(metric) for r in rows]
        
        fig = go.Figure()
        
        # Main line trace
        fig.add_trace(go.Scatter(
            x=times, 
            y=y, 
            mode='lines', 
            name=metric,
            line=dict(color='#3498db', width=2)
        ))
        
        # Add anomaly markers
        anomaly_times = [t for t, flag in zip(times, anomaly_flags) if flag]
        anomaly_y = [y[i] for i, flag in enumerate(anomaly_flags) if flag]
        
        if anomaly_times:
            # Build hover text for anomaly points
            hover_texts = []
            print(f"  Building hover texts for {metric}: {len(anomaly_times)} anomaly times, map has {len(anomaly_map)} entries")
            for t in anomaly_times:
                # Round to nearest 10 seconds to match anomaly_map keys
                t_rounded = t.replace(second=(t.second // 10) * 10, microsecond=0)
                if t_rounded in anomaly_map:
                    anom_info = anomaly_map[t_rounded]
                    # Extract just the path badges (first part before details)
                    reason_parts = anom_info['reason'].split('PATH')
                    path_only = reason_parts[0].replace('<br>', '').replace('<b>', '').replace('</b>', '').strip()
                    hover_texts.append(
                        f"⚠️ ANOMALY\n" +
                        f"Time: {t.strftime('%H:%M:%S')}\n" +
                        f"{path_only}"
                    )
                else:
                    hover_texts.append(f"⚠️ ANOMALY\nTime: {t.strftime('%H:%M:%S')}")
            
            fig.add_trace(go.Scatter(
                x=anomaly_times,
                y=anomaly_y,
                mode='markers',
                name='Anomaly',
                marker=dict(
                    size=12,
                    color='#e74c3c',
                    symbol='x',
                    line=dict(width=2, color='#c0392b')
                ),
                text=hover_texts,
                hoverinfo='text'
            ))
        
        fig.update_layout(
            title=metric.replace('_', ' ').title(),
            margin=dict(l=20, r=10, t=30, b=20),
            height=220,
            template='plotly_white',
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True),
            showlegend=False
        )
        graphs.append(dcc.Graph(figure=fig, config={'displayModeBar': False}))
    
    # Render as CSS grid
    return html.Div(graphs, style={
        'display': 'grid',
        'gridTemplateColumns': 'repeat(auto-fill, minmax(320px, 1fr))',
        'gap': '16px'
    })


# Handle device card clicks → update URL
@app.callback(
    Output('url', 'search'),
    Input({'type': 'device-card', 'device_id': ALL}, 'n_clicks'),
    State('url', 'search'),
    prevent_initial_call=True
)
def on_device_click(n_clicks_list, current_search):
    # Check if this was actually triggered by a real click
    if not ctx.triggered_id:
        return no_update
    
    # Get which card was clicked
    triggered_id = ctx.triggered_id
    if not isinstance(triggered_id, dict) or 'device_id' not in triggered_id:
        return no_update
    
    # Find the index of the triggered card to check its n_clicks
    triggered_prop = ctx.triggered[0]['prop_id']
    # triggered_prop looks like: '{"device_id":"RTU-002","type":"device-card"}.n_clicks'
    
    # Only update if we have a real click (not 0 or None)
    # The issue is cards recreating with n_clicks=0 triggers this callback
    if not any(n_clicks_list):  # All are None or 0
        return no_update
    
    device_id = triggered_id['device_id']
    new_url = f'?device={device_id}'
    
    # Don't update if URL would be the same
    if current_search == new_url:
        return no_update
    
    print(f"Device clicked: {device_id}, updating URL to {new_url}")
    return new_url


# Update label and graphs when selection or time range changes
@app.callback(
    [Output('selected-device-label', 'children'), Output('device-metric-graphs', 'children')],
    [Input('url', 'search'), Input('hours-back', 'value')],
    [State('interval-component', 'n_intervals')]
)
def update_device_detail(url_search, hours_back, _n):
    # Parse device from URL
    selected_device_id = None
    if url_search and 'device=' in url_search:
        # Extract device_id from ?device=RTU-001
        try:
            selected_device_id = url_search.split('device=')[1].split('&')[0]
        except:
            pass
    
    # If no device in URL, pick the first available
    if not selected_device_id:
        devices = get_latest_device_states()
        if not devices:
            return no_update, no_update
        selected_device_id = devices[0]['device_id']
        print(f"No device in URL, defaulting to: {selected_device_id}")
    else:
        print(f"URL has device: {selected_device_id}")
    
    rows = get_device_history(selected_device_id, hours_back or 6)
    anomalies = get_anomaly_events(selected_device_id, hours_back or 6)
    graphs = build_metric_graphs(selected_device_id, rows, anomalies)
    return selected_device_id, graphs


# Separate callback to refresh graphs on interval without changing selection
@app.callback(
    Output('device-metric-graphs', 'children', allow_duplicate=True),
    [Input('interval-component', 'n_intervals')],
    [State('url', 'search'), State('hours-back', 'value')],
    prevent_initial_call=True
)
def refresh_device_graphs(_n, url_search, hours_back):
    # Parse device from URL
    selected_device_id = None
    if url_search and 'device=' in url_search:
        try:
            selected_device_id = url_search.split('device=')[1].split('&')[0]
        except:
            pass
    
    if not selected_device_id:
        return no_update
    rows = get_device_history(selected_device_id, hours_back or 6)
    anomalies = get_anomaly_events(selected_device_id, hours_back or 6)
    return build_metric_graphs(selected_device_id, rows, anomalies)


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Starting IoT Device Monitor Dashboard")
    print("=" * 60)
    print(f"\nConnected to ScyllaDB: {scylla_hosts[0]}")
    print(f"Keyspace: {scylla_keyspace}")
    print(f"\n📊 Dashboard URL: http://127.0.0.1:8050")
    print("\nPress Ctrl+C to stop\n")
    
    app.run_server(debug=True, host='0.0.0.0', port=8050)
