#!/usr/bin/env python3
"""
IoT Device Simulator for HVAC Equipment

Simulates realistic HVAC device telemetry with:
- Stateful metric evolution (smooth, realistic changes)
- Per-device history tracking
- Configurable anomaly injection (can be 0 for normal operation)
- Targeted anomaly device IDs
- Multiple output sinks (stdout, Kafka, Kinesis)
"""

import argparse
import json
import time
import random
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Deque
from collections import deque
from dataclasses import dataclass, field
import math


@dataclass
class MetricSpec:
    """Specification for a device metric"""
    name: str
    unit: str
    min_value: float
    max_value: float
    typical_value: float
    noise_sigma: float  # Standard deviation of noise
    max_delta_per_tick: float  # Maximum change per measurement
    
    def clamp(self, value: float) -> float:
        """Clamp value to valid range"""
        return max(self.min_value, min(self.max_value, value))


@dataclass
class MetricState:
    """State for a single metric"""
    spec: MetricSpec
    current_value: float
    target_value: float
    history: Deque[float] = field(default_factory=lambda: deque(maxlen=60))
    
    def __post_init__(self):
        if not self.history:
            # Initialize with current value
            self.history.append(self.current_value)
    
    def update(self, new_target: Optional[float] = None, 
               responsiveness: float = 0.15) -> float:
        """
        Smoothly update metric value toward target
        
        Args:
            new_target: Optional new target value
            responsiveness: How quickly to move toward target (0-1)
        
        Returns:
            Updated metric value
        """
        if new_target is not None:
            self.target_value = new_target
        
        # Calculate delta toward target
        delta = responsiveness * (self.target_value - self.current_value)
        
        # Add process noise
        noise = random.gauss(0, self.spec.noise_sigma)
        delta += noise
        
        # Clamp delta to maximum rate of change
        if abs(delta) > self.spec.max_delta_per_tick:
            delta = math.copysign(self.spec.max_delta_per_tick, delta)
        
        # Update value
        new_value = self.current_value + delta
        new_value = self.spec.clamp(new_value)
        
        self.current_value = new_value
        self.history.append(new_value)
        
        return new_value


class DeviceSimulator:
    """Base class for device simulators"""
    
    def __init__(self, device_id: str, location: str, building_id: str, 
                 seed: Optional[int] = None):
        self.device_id = device_id
        self.location = location
        self.building_id = building_id
        self.device_type = self.__class__.__name__.lower().replace('simulator', '')
        
        # Set random seed for reproducibility
        if seed is None:
            seed = hash(device_id) & 0xFFFFFFFF
        self.rng = random.Random(seed)
        
        # Metric states
        self.metrics: Dict[str, MetricState] = {}
        self._initialize_metrics()
        
        # Anomaly state
        self.active_anomalies: List[str] = []
        self.anomaly_start_time: Optional[float] = None
    
    def _initialize_metrics(self):
        """Initialize metric specs and states (override in subclasses)"""
        raise NotImplementedError
    
    def _create_metric_state(self, spec: MetricSpec, 
                            initial_value: Optional[float] = None) -> MetricState:
        """Helper to create a metric state"""
        if initial_value is None:
            # Start near typical value with some randomness
            initial_value = spec.typical_value + self.rng.gauss(0, spec.noise_sigma * 2)
            initial_value = spec.clamp(initial_value)
        
        return MetricState(
            spec=spec,
            current_value=initial_value,
            target_value=initial_value
        )
    
    def tick(self) -> Dict[str, float]:
        """Generate one measurement cycle (override in subclasses)"""
        raise NotImplementedError
    
    def start_anomaly(self, anomaly_type: str):
        """Start an anomaly scenario"""
        if anomaly_type not in self.active_anomalies:
            self.active_anomalies.append(anomaly_type)
            self.anomaly_start_time = time.time()
    
    def stop_anomaly(self, anomaly_type: str):
        """Stop an anomaly scenario"""
        if anomaly_type in self.active_anomalies:
            self.active_anomalies.remove(anomaly_type)
            if not self.active_anomalies:
                self.anomaly_start_time = None
    
    def get_metric_value(self, metric_name: str) -> float:
        """Get current value of a metric"""
        return self.metrics[metric_name].current_value if metric_name in self.metrics else 0.0


class RooftopUnitSimulator(DeviceSimulator):
    """Rooftop Unit (RTU) simulator"""
    
    def _initialize_metrics(self):
        """Initialize RTU metrics"""
        specs = {
            'supply_air_temp': MetricSpec('supply_air_temp', '°F', 50, 80, 65, 0.5, 2.0),
            'return_air_temp': MetricSpec('return_air_temp', '°F', 65, 80, 72, 0.3, 1.5),
            'outdoor_air_temp': MetricSpec('outdoor_air_temp', '°F', -20, 110, 75, 1.0, 3.0),
            'supply_air_humidity': MetricSpec('supply_air_humidity', '%', 25, 65, 45, 1.0, 3.0),
            'return_air_humidity': MetricSpec('return_air_humidity', '%', 30, 70, 50, 0.8, 2.5),
            'cooling_setpoint': MetricSpec('cooling_setpoint', '°F', 70, 78, 74, 0.0, 0.5),
            'heating_setpoint': MetricSpec('heating_setpoint', '°F', 65, 73, 70, 0.0, 0.5),
            'fan_speed': MetricSpec('fan_speed', 'RPM', 0, 1800, 1500, 20, 100),
            'fan_current': MetricSpec('fan_current', 'Amps', 0, 30, 15, 0.5, 3.0),
            'compressor_status': MetricSpec('compressor_status', '0/1', 0, 1, 1, 0.0, 1.0),
            'heating_valve_position': MetricSpec('heating_valve_position', '%', 0, 100, 20, 2.0, 10.0),
            'economizer_damper_position': MetricSpec('economizer_damper_position', '%', 0, 100, 30, 2.0, 8.0),
            'filter_pressure_drop': MetricSpec('filter_pressure_drop', 'inH2O', 0.1, 2.5, 0.5, 0.02, 0.1),
            'power_consumption': MetricSpec('power_consumption', 'kW', 5, 55, 25, 1.0, 5.0),
        }
        
        for name, spec in specs.items():
            self.metrics[name] = self._create_metric_state(spec)
    
    def tick(self) -> Dict[str, float]:
        """Generate RTU measurements"""
        # Outdoor temp evolves independently
        outdoor_temp = self.metrics['outdoor_air_temp']
        outdoor_temp.update(
            outdoor_temp.target_value + self.rng.gauss(0, 0.5),
            responsiveness=0.05
        )
        
        # Cooling/heating mode based on outdoor temp and setpoints
        cooling_sp = self.metrics['cooling_setpoint'].current_value
        heating_sp = self.metrics['heating_setpoint'].current_value
        outdoor_val = outdoor_temp.current_value
        
        # Determine if cooling or heating
        cooling_needed = outdoor_val > cooling_sp + 2
        heating_needed = outdoor_val < heating_sp - 2
        
        # Compressor status
        if 'compressor_failure' in self.active_anomalies:
            compressor_target = 0.0
        elif cooling_needed:
            compressor_target = 1.0
        else:
            compressor_target = 0.0
        
        self.metrics['compressor_status'].update(compressor_target, responsiveness=1.0)
        
        # Supply air temp depends on mode
        if cooling_needed:
            supply_target = cooling_sp - 8
        elif heating_needed:
            supply_target = heating_sp + 10
        else:
            supply_target = (cooling_sp + heating_sp) / 2
        
        # Apply anomalies
        if 'sensor_drift' in self.active_anomalies:
            supply_target += 15  # Unrealistic reading
        
        self.metrics['supply_air_temp'].update(supply_target, responsiveness=0.2)
        
        # Return air temp follows room conditions
        return_target = (cooling_sp + heating_sp) / 2
        self.metrics['return_air_temp'].update(return_target, responsiveness=0.1)
        
        # Fan speed
        if 'fan_failure' in self.active_anomalies:
            fan_target = 0.0
        else:
            fan_target = 1500 if self.metrics['compressor_status'].current_value > 0.5 else 800
        
        self.metrics['fan_speed'].update(fan_target, responsiveness=0.15)
        
        # Fan current proportional to speed
        fan_speed_pct = self.metrics['fan_speed'].current_value / 1800.0
        fan_current_target = 5 + (20 * fan_speed_pct)
        self.metrics['fan_current'].update(fan_current_target, responsiveness=0.2)
        
        # Filter pressure drop
        if 'dirty_filter' in self.active_anomalies:
            filter_target = 1.8
        else:
            filter_target = 0.5 + (fan_speed_pct * 0.3)
        self.metrics['filter_pressure_drop'].update(filter_target, responsiveness=0.1)
        
        # Economizer damper
        if 'economizer_stuck' in self.active_anomalies:
            damper_target = self.metrics['economizer_damper_position'].current_value
        else:
            damper_target = 30 if outdoor_val < 65 else 10
        self.metrics['economizer_damper_position'].update(damper_target, responsiveness=0.1)
        
        # Heating valve
        heating_valve_target = 80 if heating_needed else 10
        self.metrics['heating_valve_position'].update(heating_valve_target, responsiveness=0.12)
        
        # Humidity
        self.metrics['supply_air_humidity'].update(45, responsiveness=0.08)
        self.metrics['return_air_humidity'].update(50, responsiveness=0.08)
        
        # Power consumption
        compressor_power = 20 if self.metrics['compressor_status'].current_value > 0.5 else 0
        fan_power = 5 + (fan_speed_pct * 15)
        power_target = compressor_power + fan_power
        self.metrics['power_consumption'].update(power_target, responsiveness=0.15)
        
        # Setpoints evolve slowly
        self.metrics['cooling_setpoint'].update(74, responsiveness=0.02)
        self.metrics['heating_setpoint'].update(70, responsiveness=0.02)
        
        return {name: state.current_value for name, state in self.metrics.items()}


class MakeupAirUnitSimulator(DeviceSimulator):
    """Makeup Air Unit (MAU) simulator"""
    
    def _initialize_metrics(self):
        specs = {
            'supply_air_temp': MetricSpec('supply_air_temp', '°F', 55, 85, 70, 0.5, 2.0),
            'outdoor_air_temp': MetricSpec('outdoor_air_temp', '°F', -20, 110, 75, 1.0, 3.0),
            'supply_air_humidity': MetricSpec('supply_air_humidity', '%', 25, 75, 50, 1.0, 3.0),
            'outdoor_air_humidity': MetricSpec('outdoor_air_humidity', '%', 10, 95, 60, 2.0, 5.0),
            'supply_air_flow': MetricSpec('supply_air_flow', 'CFM', 400, 5200, 2500, 50, 200),
            'fan_speed': MetricSpec('fan_speed', 'RPM', 0, 1500, 1200, 20, 100),
            'fan_vfd_frequency': MetricSpec('fan_vfd_frequency', 'Hz', 0, 60, 48, 1.0, 5.0),
            'heating_coil_temp': MetricSpec('heating_coil_temp', '°F', 70, 185, 120, 2.0, 10.0),
            'cooling_coil_temp': MetricSpec('cooling_coil_temp', '°F', 38, 68, 50, 1.0, 5.0),
            'filter_pressure_drop': MetricSpec('filter_pressure_drop', 'inH2O', 0.1, 1.8, 0.4, 0.02, 0.1),
            'static_pressure': MetricSpec('static_pressure', 'inH2O', 0.4, 3.2, 1.5, 0.1, 0.3),
            'damper_position': MetricSpec('damper_position', '%', 0, 100, 50, 2.0, 10.0),
            'gas_valve_position': MetricSpec('gas_valve_position', '%', 0, 100, 30, 2.0, 8.0),
            'power_consumption': MetricSpec('power_consumption', 'kW', 2, 32, 15, 0.5, 3.0),
        }
        for name, spec in specs.items():
            self.metrics[name] = self._create_metric_state(spec)
    
    def tick(self) -> Dict[str, float]:
        outdoor_temp = self.metrics['outdoor_air_temp']
        outdoor_temp.update(outdoor_temp.target_value + self.rng.gauss(0, 0.5), responsiveness=0.05)
        
        # Fan speed and VFD
        if 'fan_bearing_failure' in self.active_anomalies:
            fan_target = self.rng.gauss(800, 200)  # Erratic
        else:
            fan_target = 1200
        self.metrics['fan_speed'].update(fan_target, responsiveness=0.12)
        
        vfd_target = (self.metrics['fan_speed'].current_value / 1500.0) * 60
        self.metrics['fan_vfd_frequency'].update(vfd_target, responsiveness=0.15)
        
        # Heating coil
        heating_needed = outdoor_temp.current_value < 60
        if 'heating_coil_failure' in self.active_anomalies:
            heating_coil_target = 80
        elif heating_needed:
            heating_coil_target = 140
        else:
            heating_coil_target = 90
        self.metrics['heating_coil_temp'].update(heating_coil_target, responsiveness=0.1)
        
        # Supply temp
        supply_target = 70 if heating_needed else 65
        self.metrics['supply_air_temp'].update(supply_target, responsiveness=0.15)
        
        # Airflow
        if 'airflow_imbalance' in self.active_anomalies:
            flow_target = 1000
        else:
            flow_target = 2500
        self.metrics['supply_air_flow'].update(flow_target, responsiveness=0.12)
        
        # Damper
        if 'stuck_damper' in self.active_anomalies:
            damper_target = self.metrics['damper_position'].current_value
        else:
            damper_target = 70 if heating_needed else 40
        self.metrics['damper_position'].update(damper_target, responsiveness=0.1)
        
        # Other metrics
        self.metrics['outdoor_air_humidity'].update(60, responsiveness=0.05)
        self.metrics['supply_air_humidity'].update(50, responsiveness=0.08)
        self.metrics['cooling_coil_temp'].update(50, responsiveness=0.1)
        self.metrics['filter_pressure_drop'].update(0.4, responsiveness=0.05)
        self.metrics['static_pressure'].update(1.5, responsiveness=0.08)
        self.metrics['gas_valve_position'].update(50 if heating_needed else 10, responsiveness=0.1)
        self.metrics['power_consumption'].update(15, responsiveness=0.12)
        
        return {name: state.current_value for name, state in self.metrics.items()}


class ChillerSimulator(DeviceSimulator):
    """Chiller simulator"""
    
    def _initialize_metrics(self):
        specs = {
            'chilled_water_supply_temp': MetricSpec('chilled_water_supply_temp', '°F', 40, 50, 45, 0.3, 1.0),
            'chilled_water_return_temp': MetricSpec('chilled_water_return_temp', '°F', 50, 60, 55, 0.3, 1.0),
            'chilled_water_flow_rate': MetricSpec('chilled_water_flow_rate', 'GPM', 80, 1050, 500, 10, 50),
            'chilled_water_delta_t': MetricSpec('chilled_water_delta_t', '°F', 6, 16, 10, 0.3, 1.5),
            'condenser_water_supply_temp': MetricSpec('condenser_water_supply_temp', '°F', 70, 98, 85, 1.0, 3.0),
            'condenser_water_return_temp': MetricSpec('condenser_water_return_temp', '°F', 80, 108, 95, 1.0, 3.0),
            'condenser_water_flow_rate': MetricSpec('condenser_water_flow_rate', 'GPM', 140, 1250, 650, 15, 60),
            'refrigerant_pressure_evap': MetricSpec('refrigerant_pressure_evap', 'PSIG', 32, 52, 42, 0.5, 3.0),
            'refrigerant_pressure_cond': MetricSpec('refrigerant_pressure_cond', 'PSIG', 95, 185, 140, 2.0, 8.0),
            'compressor_current': MetricSpec('compressor_current', 'Amps', 45, 410, 200, 5.0, 30),
            'compressor_status': MetricSpec('compressor_status', '0/1', 0, 1, 1, 0.0, 1.0),
            'capacity_percentage': MetricSpec('capacity_percentage', '%', 0, 100, 70, 2.0, 10),
            'cop': MetricSpec('cop', 'ratio', 2.5, 7.5, 5.0, 0.1, 0.5),
            'alarm_status': MetricSpec('alarm_status', '0/1', 0, 1, 0, 0.0, 1.0),
            'power_consumption': MetricSpec('power_consumption', 'kW', 45, 820, 350, 10, 50),
        }
        for name, spec in specs.items():
            self.metrics[name] = self._create_metric_state(spec)
    
    def tick(self) -> Dict[str, float]:
        # Compressor
        if 'compressor_failure' in self.active_anomalies:
            comp_status = 0.0
            comp_current = 0.0
        else:
            comp_status = 1.0
            comp_current = 200
        self.metrics['compressor_status'].update(comp_status, responsiveness=1.0)
        self.metrics['compressor_current'].update(comp_current, responsiveness=0.2)
        
        # Chilled water temps
        if comp_status > 0.5:
            supply_target = 45
            return_target = 55
        else:
            supply_target = 50
            return_target = 52
        
        if 'refrigerant_leak' in self.active_anomalies:
            supply_target += 5  # Poor cooling
        
        self.metrics['chilled_water_supply_temp'].update(supply_target, responsiveness=0.15)
        self.metrics['chilled_water_return_temp'].update(return_target, responsiveness=0.12)
        
        delta_t = self.metrics['chilled_water_return_temp'].current_value - self.metrics['chilled_water_supply_temp'].current_value
        self.metrics['chilled_water_delta_t'].update(delta_t, responsiveness=0.2)
        
        # Flow rates
        if 'low_flow' in self.active_anomalies:
            chw_flow = 200
            cond_flow = 300
        else:
            chw_flow = 500
            cond_flow = 650
        
        self.metrics['chilled_water_flow_rate'].update(chw_flow, responsiveness=0.15)
        self.metrics['condenser_water_flow_rate'].update(cond_flow, responsiveness=0.15)
        
        # Condenser temps
        if 'fouled_condenser' in self.active_anomalies:
            cond_supply = 92
            cond_return = 102
        else:
            cond_supply = 85
            cond_return = 95
        self.metrics['condenser_water_supply_temp'].update(cond_supply, responsiveness=0.12)
        self.metrics['condenser_water_return_temp'].update(cond_return, responsiveness=0.12)
        
        # Pressures
        evap_press = 42 if comp_status > 0.5 else 35
        cond_press = 140 if comp_status > 0.5 else 100
        self.metrics['refrigerant_pressure_evap'].update(evap_press, responsiveness=0.15)
        self.metrics['refrigerant_pressure_cond'].update(cond_press, responsiveness=0.15)
        
        # Efficiency
        capacity = 70 if comp_status > 0.5 else 0
        self.metrics['capacity_percentage'].update(capacity, responsiveness=0.12)
        
        cop = 5.0 if not any(a in self.active_anomalies for a in ['fouled_condenser', 'refrigerant_leak']) else 3.5
        self.metrics['cop'].update(cop, responsiveness=0.08)
        
        # Power and alarms
        power = 350 if comp_status > 0.5 else 50
        self.metrics['power_consumption'].update(power, responsiveness=0.15)
        
        alarm = 1.0 if 'low_flow' in self.active_anomalies else 0.0
        self.metrics['alarm_status'].update(alarm, responsiveness=1.0)
        
        return {name: state.current_value for name, state in self.metrics.items()}


class CoolingTowerSimulator(DeviceSimulator):
    """Evaporator/Cooling Tower simulator"""
    
    def _initialize_metrics(self):
        specs = {
            'inlet_water_temp': MetricSpec('inlet_water_temp', '°F', 82, 108, 95, 0.5, 2.0),
            'outlet_water_temp': MetricSpec('outlet_water_temp', '°F', 72, 98, 85, 0.5, 2.0),
            'water_flow_rate': MetricSpec('water_flow_rate', 'GPM', 140, 1250, 650, 15, 60),
            'wet_bulb_temp': MetricSpec('wet_bulb_temp', '°F', 38, 82, 65, 0.8, 3.0),
            'dry_bulb_temp': MetricSpec('dry_bulb_temp', '°F', 48, 102, 75, 1.0, 3.0),
            'approach_temp': MetricSpec('approach_temp', '°F', 4, 16, 8, 0.3, 1.5),
            'range_temp': MetricSpec('range_temp', '°F', 6, 16, 10, 0.3, 1.5),
            'fan_speed': MetricSpec('fan_speed', 'RPM', 0, 420, 300, 10, 50),
            'fan_current': MetricSpec('fan_current', 'Amps', 3, 42, 20, 0.5, 3.0),
            'sump_water_level': MetricSpec('sump_water_level', '%', 35, 95, 70, 1.0, 5.0),
            'makeup_water_flow': MetricSpec('makeup_water_flow', 'GPM', 0, 52, 10, 1.0, 5.0),
            'blowdown_valve_position': MetricSpec('blowdown_valve_position', '%', 0, 100, 20, 1.0, 8.0),
            'vibration_level': MetricSpec('vibration_level', 'mm/s', 0, 12, 3, 0.3, 2.0),
            'power_consumption': MetricSpec('power_consumption', 'kW', 1, 52, 18, 1.0, 5.0),
        }
        for name, spec in specs.items():
            self.metrics[name] = self._create_metric_state(spec)
    
    def tick(self) -> Dict[str, float]:
        # Outdoor conditions
        wet_bulb = self.metrics['wet_bulb_temp']
        wet_bulb.update(wet_bulb.target_value + self.rng.gauss(0, 0.3), responsiveness=0.05)
        
        dry_bulb = self.metrics['dry_bulb_temp']
        dry_bulb.update(dry_bulb.target_value + self.rng.gauss(0, 0.5), responsiveness=0.05)
        
        # Fan
        if 'fan_failure' in self.active_anomalies:
            fan_target = 0.0
        else:
            fan_target = 300
        self.metrics['fan_speed'].update(fan_target, responsiveness=0.15)
        
        fan_pct = self.metrics['fan_speed'].current_value / 420.0
        fan_current = 5 + (35 * fan_pct)
        self.metrics['fan_current'].update(fan_current, responsiveness=0.2)
        
        # Water temps
        inlet_target = 95
        if 'scale_buildup' in self.active_anomalies:
            outlet_target = 90  # Poor heat transfer
        else:
            outlet_target = 85
        
        self.metrics['inlet_water_temp'].update(inlet_target, responsiveness=0.12)
        self.metrics['outlet_water_temp'].update(outlet_target, responsiveness=0.12)
        
        # Approach and range
        approach = self.metrics['outlet_water_temp'].current_value - wet_bulb.current_value
        range_val = self.metrics['inlet_water_temp'].current_value - self.metrics['outlet_water_temp'].current_value
        self.metrics['approach_temp'].update(approach, responsiveness=0.15)
        self.metrics['range_temp'].update(range_val, responsiveness=0.15)
        
        # Water level
        if 'low_water_level' in self.active_anomalies:
            level_target = 35
        else:
            level_target = 70
        self.metrics['sump_water_level'].update(level_target, responsiveness=0.1)
        
        # Makeup water
        makeup = 10 if fan_pct > 0.5 else 5
        self.metrics['makeup_water_flow'].update(makeup, responsiveness=0.12)
        
        # Other metrics
        self.metrics['water_flow_rate'].update(650, responsiveness=0.12)
        self.metrics['blowdown_valve_position'].update(20, responsiveness=0.08)
        
        vibration = 8 if 'fan_failure' in self.active_anomalies else 3
        self.metrics['vibration_level'].update(vibration, responsiveness=0.15)
        
        power = 18 * fan_pct if fan_pct > 0.1 else 2
        self.metrics['power_consumption'].update(power, responsiveness=0.15)
        
        return {name: state.current_value for name, state in self.metrics.items()}


class AirCompressorSimulator(DeviceSimulator):
    """Air Compressor simulator"""
    
    def _initialize_metrics(self):
        specs = {
            'discharge_pressure': MetricSpec('discharge_pressure', 'PSIG', 85, 130, 110, 1.0, 5.0),
            'inlet_pressure': MetricSpec('inlet_pressure', 'PSIG', -2, 16, 0, 0.2, 1.0),
            'discharge_temp': MetricSpec('discharge_temp', '°F', 145, 255, 200, 2.0, 10),
            'ambient_temp': MetricSpec('ambient_temp', '°F', 48, 102, 75, 0.5, 2.0),
            'motor_current': MetricSpec('motor_current', 'Amps', 15, 105, 60, 2.0, 10),
            'motor_speed': MetricSpec('motor_speed', 'RPM', 0, 3650, 1800, 50, 200),
            'oil_pressure': MetricSpec('oil_pressure', 'PSIG', 25, 62, 45, 0.5, 3.0),
            'oil_temp': MetricSpec('oil_temp', '°F', 115, 205, 160, 1.0, 5.0),
            'cooling_water_temp': MetricSpec('cooling_water_temp', '°F', 58, 98, 75, 0.5, 3.0),
            'air_flow_rate': MetricSpec('air_flow_rate', 'CFM', 45, 520, 250, 10, 40),
            'load_percentage': MetricSpec('load_percentage', '%', 0, 100, 70, 2.0, 10),
            'run_hours': MetricSpec('run_hours', 'hours', 0, 50000, 5000, 0.0, 0.02),
            'vibration_level': MetricSpec('vibration_level', 'mm/s', 0, 16, 5, 0.3, 2.0),
            'power_consumption': MetricSpec('power_consumption', 'kW', 8, 155, 80, 2.0, 10),
        }
        for name, spec in specs.items():
            self.metrics[name] = self._create_metric_state(spec)
    
    def tick(self) -> Dict[str, float]:
        # Discharge pressure
        if 'pressure_leak' in self.active_anomalies:
            pressure_target = 95  # Can't reach setpoint
        else:
            pressure_target = 110
        self.metrics['discharge_pressure'].update(pressure_target, responsiveness=0.12)
        
        # Motor
        running = self.metrics['discharge_pressure'].current_value < 120
        if 'motor_overload' in self.active_anomalies:
            motor_current = 95
        elif running:
            motor_current = 60
        else:
            motor_current = 20
        self.metrics['motor_current'].update(motor_current, responsiveness=0.15)
        
        motor_speed = 1800 if running else 0
        self.metrics['motor_speed'].update(motor_speed, responsiveness=0.2)
        
        # Temps
        if 'overheating' in self.active_anomalies:
            discharge_temp = 240
        else:
            discharge_temp = 200
        self.metrics['discharge_temp'].update(discharge_temp, responsiveness=0.12)
        
        # Oil system
        if 'oil_system_failure' in self.active_anomalies:
            oil_pressure = 28
            oil_temp = 190
        else:
            oil_pressure = 45
            oil_temp = 160
        self.metrics['oil_pressure'].update(oil_pressure, responsiveness=0.12)
        self.metrics['oil_temp'].update(oil_temp, responsiveness=0.1)
        
        # Other metrics
        self.metrics['inlet_pressure'].update(0, responsiveness=0.1)
        self.metrics['ambient_temp'].update(75, responsiveness=0.05)
        self.metrics['cooling_water_temp'].update(75, responsiveness=0.1)
        
        load_pct = 70 if running else 10
        self.metrics['load_percentage'].update(load_pct, responsiveness=0.12)
        
        airflow = 250 if running else 50
        self.metrics['air_flow_rate'].update(airflow, responsiveness=0.15)
        
        vibration = 12 if 'motor_overload' in self.active_anomalies else 5
        self.metrics['vibration_level'].update(vibration, responsiveness=0.15)
        
        power = 80 if running else 10
        self.metrics['power_consumption'].update(power, responsiveness=0.15)
        
        # Run hours increment slowly
        self.metrics['run_hours'].update(
            self.metrics['run_hours'].current_value + (0.016 if running else 0),
            responsiveness=1.0
        )
        
        return {name: state.current_value for name, state in self.metrics.items()}


# Device type registry
DEVICE_TYPES = {
    'rooftop_unit': RooftopUnitSimulator,
    'makeup_air_unit': MakeupAirUnitSimulator,
    'chiller': ChillerSimulator,
    'cooling_tower': CoolingTowerSimulator,
    'air_compressor': AirCompressorSimulator,
}


def generate_messages(device: DeviceSimulator) -> List[Dict]:
    """Generate individual metric messages for one tick"""
    metrics = device.tick()
    timestamp = datetime.now(timezone.utc).isoformat()
    
    messages = []
    for metric_name, metric_value in metrics.items():
        spec = device.metrics[metric_name].spec
        message = {
            'device_id': device.device_id,
            'device_type': device.device_type,
            'timestamp': timestamp,
            'metric_name': metric_name,
            'metric_value': round(metric_value, 2),
            'unit': spec.unit,
            'location': device.location,
            'building_id': device.building_id,
        }
        messages.append(message)
    
    return messages


def should_trigger_anomaly(device_id: str, anomaly_rate: float, 
                          anomaly_device_ids: Optional[List[str]]) -> bool:
    """Determine if device should have an anomaly"""
    if anomaly_rate == 0.0:
        return False
    
    if anomaly_device_ids and device_id not in anomaly_device_ids:
        return False
    
    return random.random() < anomaly_rate


def main():
    parser = argparse.ArgumentParser(description='HVAC IoT Device Simulator')
    parser.add_argument('--device-type', required=True, 
                       choices=list(DEVICE_TYPES.keys()),
                       help='Type of device to simulate')
    parser.add_argument('--device-id', required=True,
                       help='Unique device identifier')
    parser.add_argument('--location', default='building-A',
                       help='Device location')
    parser.add_argument('--building-id', default='bldg-001',
                       help='Building identifier')
    parser.add_argument('--interval', type=float, default=60.0,
                       help='Measurement interval in seconds')
    parser.add_argument('--duration', type=int, default=0,
                       help='Duration to run in seconds (0 = infinite)')
    parser.add_argument('--anomaly-rate', type=float, default=0.0,
                       help='Probability of anomaly per check (0.0-1.0, default 0.0)')
    parser.add_argument('--no-anomalies', action='store_true',
                       help='Disable all anomalies (overrides anomaly-rate)')
    parser.add_argument('--anomaly-device-ids', type=str,
                       help='Comma-separated list of device IDs that can have anomalies')
    parser.add_argument('--seed', type=int,
                       help='Random seed for reproducibility')
    parser.add_argument('--sink', default='stdout',
                       choices=['stdout', 'kafka', 'kinesis'],
                       help='Output sink')
    
    args = parser.parse_args()
    
    # Parse anomaly device IDs
    anomaly_device_ids = None
    if args.anomaly_device_ids:
        anomaly_device_ids = [x.strip() for x in args.anomaly_device_ids.split(',')]
    
    # Handle no-anomalies flag
    if args.no_anomalies:
        anomaly_rate = 0.0
    else:
        anomaly_rate = args.anomaly_rate
    
    # Create device simulator
    device_class = DEVICE_TYPES[args.device_type]
    device = device_class(
        device_id=args.device_id,
        location=args.location,
        building_id=args.building_id,
        seed=args.seed
    )
    
    print(f"Starting {args.device_type} simulator: {args.device_id}", file=sys.stderr)
    print(f"Location: {args.location}, Building: {args.building_id}", file=sys.stderr)
    print(f"Interval: {args.interval}s, Anomaly rate: {anomaly_rate}", file=sys.stderr)
    if anomaly_device_ids:
        print(f"Anomaly targets: {', '.join(anomaly_device_ids)}", file=sys.stderr)
    print("---", file=sys.stderr)
    
    start_time = time.time()
    tick_count = 0
    
    try:
        while True:
            # Check if we should trigger an anomaly
            if tick_count % 10 == 0:  # Check every 10 ticks
                if should_trigger_anomaly(args.device_id, anomaly_rate, anomaly_device_ids):
                    if not device.active_anomalies:
                        # Pick a random anomaly for this device type
                        anomalies = ['sensor_drift', 'fan_failure', 'dirty_filter', 
                                    'economizer_stuck', 'compressor_failure']
                        anomaly = random.choice(anomalies)
                        device.start_anomaly(anomaly)
                        print(f"[{args.device_id}] Started anomaly: {anomaly}", file=sys.stderr)
                elif device.active_anomalies and random.random() < 0.2:
                    # 20% chance to stop existing anomaly
                    anomaly = device.active_anomalies[0]
                    device.stop_anomaly(anomaly)
                    print(f"[{args.device_id}] Stopped anomaly: {anomaly}", file=sys.stderr)
            
            # Generate measurements
            messages = generate_messages(device)
            
            # Output messages
            if args.sink == 'stdout':
                for msg in messages:
                    print(json.dumps(msg))
                    sys.stdout.flush()
            # TODO: Add Kafka/Kinesis sinks
            
            tick_count += 1
            
            # Check duration
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break
            
            # Sleep until next interval
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\nStopping {args.device_id} after {tick_count} measurements", file=sys.stderr)


if __name__ == '__main__':
    main()
