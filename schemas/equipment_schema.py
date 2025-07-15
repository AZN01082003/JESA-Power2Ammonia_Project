# schemas/equipment_schema.py

from dataclasses import dataclass, asdict
from typing import Dict, Optional, Any, List
from datetime import datetime
import json
from enum import Enum

class EquipmentStatus(Enum):
    RUNNING = "running"
    IDLE = "idle"
    MAINTENANCE = "maintenance"
    FAULT = "fault"
    STARTING = "starting"
    STOPPING = "stopping"

class MaintenanceType(Enum):
    NONE = "none"
    LIGHT = "light"
    MAJOR = "major"
    EMERGENCY = "emergency"

@dataclass
class ElectrolyzerData:
    """Schema for electrolyzer telemetry data"""
    
    # Metadata
    timestamp: datetime
    equipment_id: str
    location_id: str
    
    # Operational status
    status: EquipmentStatus
    operating_hours: float              # Total operating hours
    cycles_count: int                   # Start/stop cycles
    
    # Performance metrics
    current_power: float                # MW (actual power consumption)
    rated_power: float                  # MW (rated power)
    load_factor: float                  # 0-1 (current/rated)
    efficiency: float                   # 0-1 (actual efficiency)
    efficiency_nominal: float           # 0-1 (nominal efficiency)
    
    # Process parameters
    h2_production_rate: float           # kg/h
    h2_production_cumulative: float     # kg (total produced)
    stack_voltage: float                # V
    stack_current: float                # A
    stack_temperature: float            # °C
    
    # System health
    stack_degradation: float            # 0-1 (1 = new, 0 = end of life)
    membrane_resistance: float          # Ohm
    gas_purity_h2: float               # % (H₂ purity)
    water_consumption: float            # L/h
    
    # Maintenance indicators
    maintenance_due: MaintenanceType
    hours_since_maintenance: float
    next_maintenance_hours: float
    fault_codes: List[str]
    
    # Environmental conditions
    ambient_temperature: float          # °C
    cooling_water_temp: float          # °C
    system_pressure: float             # bar
    
    # Derived metrics
    specific_energy_consumption: float  # kWh/kg H₂
    availability: float                # 0-1
    reliability_score: float           # 0-1
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['status'] = self.status.value
        data['maintenance_due'] = self.maintenance_due.value
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class ASUData:
    """Schema for Air Separation Unit telemetry data"""
    
    # Metadata
    timestamp: datetime
    equipment_id: str
    location_id: str
    
    # Operational status
    status: EquipmentStatus
    operating_hours: float
    
    # Performance metrics
    power_consumption: float            # MW
    n2_production_rate: float          # Nm³/h
    o2_production_rate: float          # Nm³/h
    production_efficiency: float        # 0-1
    
    # Process parameters
    n2_purity: float                   # %
    o2_purity: float                   # %
    air_intake_flow: float             # Nm³/h
    compressor_stages: int
    
    # System health
    compressor_efficiency: float        # 0-1
    heat_exchanger_effectiveness: float # 0-1
    molecular_sieve_condition: float    # 0-1 (1=new, 0=needs replacement)
    filter_pressure_drop: float        # bar
    
    # Process conditions
    inlet_air_pressure: float          # bar
    inlet_air_temperature: float       # °C
    distillation_column_pressure: float # bar
    cooling_water_temperature: float   # °C
    
    # Energy consumption breakdown
    compressor_power: float            # MW
    refrigeration_power: float         # MW
    auxiliary_power: float             # MW
    
    # Maintenance indicators
    maintenance_due: MaintenanceType
    hours_since_maintenance: float
    filter_replacement_due: bool
    molecular_sieve_regeneration_due: bool
    
    # Quality metrics
    product_quality_score: float       # 0-1
    process_stability: float           # 0-1
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['status'] = self.status.value
        data['maintenance_due'] = self.maintenance_due.value
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class HaberBoschData:
    """Schema for Haber-Bosch reactor telemetry data"""
    
    # Metadata
    timestamp: datetime
    equipment_id: str
    location_id: str
    
    # Operational status
    status: EquipmentStatus
    operating_hours: float
    
    # Performance metrics
    nh3_production_rate: float         # t/h
    nh3_production_cumulative: float   # t (total produced)
    conversion_efficiency: float        # 0-1 (N₂+H₂ → NH₃)
    energy_consumption: float          # MW
    
    # Reactant inputs
    n2_feed_rate: float               # kg/h
    h2_feed_rate: float               # kg/h
    n2_h2_ratio: float                # actual ratio
    optimal_n2_h2_ratio: float        # optimal ratio (usually 1:3)
    
    # Process conditions
    reactor_temperature: float         # °C
    reactor_pressure: float           # bar
    catalyst_bed_temperature: float   # °C
    cooling_water_flow: float         # m³/h
    
    # Catalyst health
    catalyst_activity: float          # 0-1 (1=new, 0=deactivated)
    catalyst_age: float               # hours
    catalyst_replacement_due: bool
    selectivity: float                # 0-1 (NH₃ selectivity)
    
    # Product quality
    nh3_purity: float                 # %
    water_content: float              # ppm
    unreacted_gases: float            # %
    
    # System efficiency
    heat_recovery_efficiency: float    # 0-1
    compression_efficiency: float      # 0-1
    separation_efficiency: float       # 0-1
    
    # Maintenance indicators
    maintenance_due: MaintenanceType
    hours_since_maintenance: float
    pressure_vessel_inspection_due: bool
    
    # Environmental impact
    co2_emissions: float              # kg/h
    water_usage: float                # L/h
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['status'] = self.status.value
        data['maintenance_due'] = self.maintenance_due.value
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class BatteryStorageData:
    """Schema for battery storage system telemetry data"""
    
    # Metadata
    timestamp: datetime
    equipment_id: str
    location_id: str
    
    # Operational status
    status: EquipmentStatus
    operating_hours: float
    
    # Energy metrics
    state_of_charge: float            # 0-1 (SOC)
    capacity_available: float         # MWh
    capacity_total: float             # MWh
    power_input: float                # MW (positive = charging)
    power_output: float               # MW (positive = discharging)
    
    # Performance metrics
    charging_efficiency: float        # 0-1
    discharging_efficiency: float     # 0-1
    round_trip_efficiency: float      # 0-1
    
    # Battery health
    state_of_health: float            # 0-1 (capacity degradation)
    cycle_count: int                  # Total charge/discharge cycles
    calendar_age: float               # days since installation
    capacity_fade: float              # 0-1 (capacity lost due to aging)
    
    # Thermal management
    battery_temperature: float        # °C
    cooling_system_power: float       # kW
    thermal_runaway_risk: float       # 0-1 (risk score)
    
    # Electrical parameters
    voltage: float                    # V
    current: float                    # A
    internal_resistance: float        # Ohm
    
    # Safety metrics
    safety_status: str               # "normal", "warning", "critical"
    fire_detection_status: bool
    gas_detection_status: bool
    
    # Maintenance indicators
    maintenance_due: MaintenanceType
    balancing_required: bool
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['status'] = self.status.value
        data['maintenance_due'] = self.maintenance_due.value
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class H2StorageData:
    """Schema for hydrogen storage system telemetry data"""
    
    # Metadata
    timestamp: datetime
    equipment_id: str
    location_id: str
    
    # Operational status
    status: EquipmentStatus
    operating_hours: float
    
    # Storage metrics
    h2_mass_stored: float             # kg
    storage_capacity: float           # kg
    fill_level: float                 # 0-1
    pressure: float                   # bar
    temperature: float                # °C
    
    # Flow rates
    h2_input_rate: float              # kg/h (from electrolyzer)
    h2_output_rate: float             # kg/h (to Haber-Bosch)
    
    # Storage performance
    compression_efficiency: float     # 0-1
    storage_efficiency: float         # 0-1 (accounting for losses)
    leak_rate: float                  # kg/h (should be minimal)
    
    # Safety metrics
    pressure_safety_margin: float     # bar (margin from max pressure)
    gas_detection_level: float        # ppm (H₂ in air)
    ventilation_status: bool
    emergency_vent_status: bool
    
    # System health
    compressor_condition: float       # 0-1
    tank_integrity: float             # 0-1
    valve_condition: float            # 0-1
    
    # Maintenance indicators
    maintenance_due: MaintenanceType
    pressure_test_due: bool
    valve_inspection_due: bool
    
    # Environmental conditions
    ambient_temperature: float        # °C
    humidity: float                   # %
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['status'] = self.status.value
        data['maintenance_due'] = self.maintenance_due.value
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class SystemOverviewData:
    """Schema for overall system performance data"""
    
    # Metadata
    timestamp: datetime
    system_id: str
    location_id: str
    
    # Overall performance
    total_power_consumption: float     # MW
    renewable_power_available: float   # MW
    grid_power_usage: float           # MW
    power_utilization_efficiency: float # 0-1
    
    # Production metrics
    nh3_production_rate: float        # t/h
    h2_production_rate: float         # kg/h
    n2_production_rate: float         # Nm³/h
    
    # System efficiency
    overall_efficiency: float         # 0-1 (renewable energy → NH₃)
    capacity_factor: float            # 0-1
    
    # Availability metrics
    system_availability: float        # 0-1
    planned_downtime: float           # hours
    unplanned_downtime: float         # hours
    
    # Economic metrics
    operational_cost: float           # €/h
    maintenance_cost: float           # €/h
    energy_cost: float               # €/MWh
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())