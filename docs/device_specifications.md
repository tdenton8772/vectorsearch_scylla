# HVAC IoT Device Specifications

## Device Types Overview

| Device Type | Typical Location | Primary Function | Measurement Frequency |
|-------------|------------------|------------------|----------------------|
| Rooftop Unit (RTU) | Building rooftops | Heating/cooling, air circulation | Every 60 seconds |
| Makeup Air Unit (MAU) | Near exhaust systems | Fresh air supply, pressurization | Every 60 seconds |
| Chiller | Mechanical room/outdoors | Cooling water for HVAC | Every 30 seconds |
| Evaporator/Cooling Tower | Outdoors | Heat rejection via evaporation | Every 60 seconds |
| Air Compressor | Utility room | Compressed air for pneumatics | Every 30 seconds |

---

## 1. Rooftop Unit (RTU)

### Description
Self-contained HVAC system on building roofs. Provides heating, cooling, and ventilation.

### Metrics

| Metric | Unit | Normal Range | Description |
|--------|------|--------------|-------------|
| `supply_air_temp` | °F | 55-75 | Temperature of air being supplied to building |
| `return_air_temp` | °F | 68-78 | Temperature of air returning from building |
| `outdoor_air_temp` | °F | -20 to 110 | Ambient outdoor temperature |
| `supply_air_humidity` | % | 30-60 | Relative humidity of supply air |
| `return_air_humidity` | % | 35-65 | Relative humidity of return air |
| `cooling_setpoint` | °F | 72-76 | Target cooling temperature |
| `heating_setpoint` | °F | 68-72 | Target heating temperature |
| `fan_speed` | RPM | 0-1800 | Supply fan rotation speed |
| `fan_current` | Amps | 5-25 | Electrical current draw of fan motor |
| `compressor_status` | 0/1 | 0 or 1 | Compressor on (1) or off (0) |
| `heating_valve_position` | % | 0-100 | Hot water/gas valve opening percentage |
| `economizer_damper_position` | % | 0-100 | Outside air damper position |
| `filter_pressure_drop` | inH2O | 0.1-2.0 | Pressure drop across air filter |
| `power_consumption` | kW | 5-50 | Total electrical power draw |

### Anomaly Scenarios
- **Fan Failure**: fan_speed drops to 0, fan_current drops
- **Refrigerant Leak**: compressor runs constantly, poor cooling
- **Dirty Filter**: High filter_pressure_drop, reduced airflow
- **Economizer Stuck**: Damper position doesn't change with conditions
- **Sensor Drift**: Unrealistic temp/humidity readings

---

## 2. Makeup Air Unit (MAU)

### Description
Brings in fresh outdoor air, conditions it, and supplies to building to maintain positive pressure.

### Metrics

| Metric | Unit | Normal Range | Description |
|--------|------|--------------|-------------|
| `supply_air_temp` | °F | 60-80 | Temperature of conditioned outdoor air |
| `outdoor_air_temp` | °F | -20 to 110 | Ambient outdoor temperature |
| `supply_air_humidity` | % | 30-70 | Relative humidity after conditioning |
| `outdoor_air_humidity` | % | 10-95 | Outdoor relative humidity |
| `supply_air_flow` | CFM | 500-5000 | Cubic feet per minute of airflow |
| `fan_speed` | RPM | 0-1500 | Supply fan rotation speed |
| `fan_vfd_frequency` | Hz | 0-60 | Variable frequency drive setting |
| `heating_coil_temp` | °F | 80-180 | Temperature of heating coil |
| `cooling_coil_temp` | °F | 40-65 | Temperature of cooling coil |
| `filter_pressure_drop` | inH2O | 0.1-1.5 | Pressure drop across filters |
| `static_pressure` | inH2O | 0.5-3.0 | Duct static pressure |
| `damper_position` | % | 0-100 | Outside air damper opening |
| `gas_valve_position` | % | 0-100 | Gas heating valve (if applicable) |
| `power_consumption` | kW | 3-30 | Total electrical power |

### Anomaly Scenarios
- **Heating Coil Failure**: heating_coil_temp doesn't rise
- **Fan Bearing Failure**: Vibration, noise (simulated via erratic RPM)
- **Stuck Damper**: damper_position frozen
- **VFD Malfunction**: Erratic fan_vfd_frequency
- **Airflow Imbalance**: supply_air_flow out of normal range

---

## 3. Chiller

### Description
Large refrigeration system that cools water for distribution to building cooling coils.

### Metrics

| Metric | Unit | Normal Range | Description |
|--------|------|--------------|-------------|
| `chilled_water_supply_temp` | °F | 42-48 | Temperature of water leaving chiller |
| `chilled_water_return_temp` | °F | 52-58 | Temperature of water returning to chiller |
| `chilled_water_flow_rate` | GPM | 100-1000 | Gallons per minute of water flow |
| `chilled_water_delta_t` | °F | 8-14 | Temperature difference (return - supply) |
| `condenser_water_supply_temp` | °F | 75-95 | Water temp entering condenser |
| `condenser_water_return_temp` | °F | 85-105 | Water temp leaving condenser |
| `condenser_water_flow_rate` | GPM | 150-1200 | Condenser water flow |
| `refrigerant_pressure_evap` | PSIG | 35-50 | Evaporator refrigerant pressure |
| `refrigerant_pressure_cond` | PSIG | 100-180 | Condenser refrigerant pressure |
| `compressor_current` | Amps | 50-400 | Compressor electrical current |
| `compressor_status` | 0/1 | 0 or 1 | Compressor running status |
| `capacity_percentage` | % | 0-100 | Chiller load percentage |
| `cop` | ratio | 3-7 | Coefficient of Performance (efficiency) |
| `alarm_status` | 0/1 | 0 or 1 | Any active alarms |
| `power_consumption` | kW | 50-800 | Total electrical power |

### Anomaly Scenarios
- **Refrigerant Leak**: Low evap pressure, poor efficiency
- **Fouled Condenser**: High condenser temps, low efficiency
- **Low Flow**: Reduced flow rates, alarms
- **Compressor Failure**: Zero current, no cooling
- **Inefficient Operation**: Low COP, high power consumption

---

## 4. Evaporator/Cooling Tower

### Description
Rejects heat from condenser water through evaporative cooling.

### Metrics

| Metric | Unit | Normal Range | Description |
|--------|------|--------------|-------------|
| `inlet_water_temp` | °F | 85-105 | Water temperature entering tower |
| `outlet_water_temp` | °F | 75-95 | Water temperature leaving tower |
| `water_flow_rate` | GPM | 150-1200 | Water flow through tower |
| `wet_bulb_temp` | °F | 40-80 | Outdoor wet bulb temperature |
| `dry_bulb_temp` | °F | 50-100 | Outdoor dry bulb temperature |
| `approach_temp` | °F | 5-15 | Difference between outlet water and wet bulb |
| `range_temp` | °F | 8-15 | Inlet temp - outlet temp |
| `fan_speed` | RPM | 0-400 | Cooling tower fan speed |
| `fan_current` | Amps | 5-40 | Fan motor current draw |
| `sump_water_level` | % | 40-90 | Water level in basin |
| `makeup_water_flow` | GPM | 0-50 | Fresh water added to compensate evaporation |
| `blowdown_valve_position` | % | 0-100 | Valve for water quality control |
| `vibration_level` | mm/s | 0-10 | Fan/structure vibration |
| `power_consumption` | kW | 2-50 | Total electrical power |

### Anomaly Scenarios
- **Fan Failure**: fan_speed = 0, poor cooling
- **Low Water Level**: sump_water_level < 40%, risk of pump damage
- **Scale Buildup**: Poor heat transfer, high approach temp
- **Fill Material Clogging**: Reduced water distribution
- **Excessive Drift**: High makeup water usage

---

## 5. Air Compressor

### Description
Generates compressed air for pneumatic controls, actuators, and tools.

### Metrics

| Metric | Unit | Normal Range | Description |
|--------|------|--------------|-------------|
| `discharge_pressure` | PSIG | 90-125 | Compressed air output pressure |
| `inlet_pressure` | PSIG | 0-15 | Atmospheric inlet pressure |
| `discharge_temp` | °F | 150-250 | Temperature of compressed air |
| `ambient_temp` | °F | 50-100 | Room/outdoor temperature |
| `motor_current` | Amps | 20-100 | Compressor motor current |
| `motor_speed` | RPM | 0-3600 | Motor rotation speed |
| `oil_pressure` | PSIG | 30-60 | Lubrication oil pressure |
| `oil_temp` | °F | 120-200 | Lubrication oil temperature |
| `cooling_water_temp` | °F | 60-95 | Cooling water temperature (if water-cooled) |
| `air_flow_rate` | CFM | 50-500 | Cubic feet per minute of air output |
| `load_percentage` | % | 0-100 | Compressor load/capacity utilization |
| `run_hours` | hours | 0-50000 | Total operating hours |
| `vibration_level` | mm/s | 0-15 | Vibration magnitude |
| `power_consumption` | kW | 10-150 | Total electrical power |

### Anomaly Scenarios
- **Pressure Leak**: Compressor runs constantly, never reaches setpoint
- **Oil System Failure**: Low oil_pressure or high oil_temp
- **Overheating**: High discharge_temp
- **Motor Overload**: High motor_current
- **Excessive Cycling**: Frequent start/stops (pressure oscillation)

---

## Message Format

### Option 1: Single Metric per Message (Recommended for Phase 1)
```json
{
  "device_id": "rtu-001",
  "device_type": "rooftop_unit",
  "timestamp": "2025-11-17T17:55:00Z",
  "metric_name": "supply_air_temp",
  "metric_value": 68.5,
  "unit": "°F",
  "location": "building-A-roof",
  "building_id": "bldg-a-001"
}
```

**Pros**: Easier to process, better for streaming, simpler anomaly detection per metric
**Cons**: More messages, higher volume

### Option 2: All Metrics per Message
```json
{
  "device_id": "rtu-001",
  "device_type": "rooftop_unit",
  "timestamp": "2025-11-17T17:55:00Z",
  "location": "building-A-roof",
  "building_id": "bldg-a-001",
  "metrics": {
    "supply_air_temp": 68.5,
    "return_air_temp": 72.3,
    "fan_speed": 1650,
    "compressor_status": 1,
    "power_consumption": 28.4
  }
}
```

**Pros**: Fewer messages, complete snapshot
**Cons**: Harder to detect individual metric anomalies

---

## Next Steps

1. **Implement device simulators** for each type
2. **Create realistic value generators** with time-series behavior
3. **Add anomaly injection** logic
4. **Test data generation** locally before streaming

Recommendation: Start with **Option 1 (single metric per message)** for cleaner anomaly detection.
