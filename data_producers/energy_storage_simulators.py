# data_producers/energy_storage_simulators.py

import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math
import random

from config.data_config import EQUIPMENT_CONFIG
from schemas.equipment_schema import BatteryStorageData, H2StorageData, EquipmentStatus, MaintenanceType

class BatteryStorageSimulator:
    """
    Battery storage system simulator with realistic degradation patterns,
    thermal management, and safety considerations
    """
    
    def __init__(self, equipment_id: str, config=None, location_id: str = "plant_001"):
        self.equipment_id = equipment_id
        self.location_id = location_id
        self.config = config or EQUIPMENT_CONFIG
        self.current_time = datetime.now()
        
        # Initialize equipment state
        self.status = EquipmentStatus.IDLE
        self.operating_hours = 0.0
        self.hours_since_maintenance = 0.0
        
        # Battery state
        self.state_of_charge = 0.5  # 50% initial SOC
        self.capacity_total = self.config.battery_capacity  # MWh
        self.capacity_available = self.capacity_total  # MWh (accounting for degradation)
        self.state_of_health = 1.0  # 100% initial health
        self.cycle_count = 0
        self.calendar_age = 0  # days
        self.capacity_fade = 0.0  # capacity lost due to aging
        
        # Power flows
        self.power_input = 0.0  # MW (positive = charging)
        self.power_output = 0.0  # MW (positive = discharging)
        
        # Performance parameters
        self.charging_efficiency = self.config.battery_efficiency
        self.discharging_efficiency = self.config.battery_efficiency
        self.round_trip_efficiency = self.charging_efficiency * self.discharging_efficiency
        
        # Thermal management
        self.battery_temperature = 25.0  # °C
        self.cooling_system_power = 0.0  # kW
        self.thermal_runaway_risk = 0.0  # Risk score 0-1
        
        # Electrical parameters
        self.voltage = 800.0  # V (nominal)
        self.current = 0.0  # A
        self.internal_resistance = 0.01  # Ohm
        
        # Safety metrics
        self.safety_status = "normal"
        self.fire_detection_status = False
        self.gas_detection_status = False
        
        # Maintenance indicators
        self.maintenance_due = MaintenanceType.NONE
        self.balancing_required = False
        
        # State tracking
        self.last_soc = self.state_of_charge
        self.charging_cycles = 0.0
        
        # Random number generator
        self.rng = np.random.RandomState()
    
    def update_degradation(self, power_level: float, temperature: float, dt_hours: float):
        """
        Update battery degradation based on operating conditions
        """
        # Calendar aging (time-based degradation)
        calendar_aging_rate = 0.00001  # per hour
        temperature_factor = math.exp((temperature - 25) / 40)  # Arrhenius relationship
        
        # Cycle aging (usage-based degradation)
        cycle_aging_rate = 0.0001  # per equivalent cycle
        depth_of_discharge = abs(self.state_of_charge - self.last_soc)
        
        # C-rate stress (high power = faster degradation)
        c_rate = abs(power_level) / self.capacity_total if self.capacity_total > 0 else 0
        c_rate_factor = 1.0 + c_rate * 0.5
        
        # Apply degradation
        calendar_degradation = calendar_aging_rate * temperature_factor * dt_hours
        cycle_degradation = cycle_aging_rate * depth_of_discharge * c_rate_factor
        
        total_degradation = calendar_degradation + cycle_degradation
        self.capacity_fade = min(0.3, self.capacity_fade + total_degradation)
        
        # Update state of health
        self.state_of_health = 1.0 - self.capacity_fade
        self.capacity_available = self.capacity_total * self.state_of_health
        
        # Update internal resistance (increases with aging)
        self.internal_resistance = 0.01 + self.capacity_fade * 0.05
        
        # Update calendar age
        self.calendar_age += dt_hours / 24.0  # Convert to days
    
    def calculate_thermal_behavior(self, power_level: float, ambient_temp: float) -> Dict[str, float]:
        """
        Calculate battery thermal behavior and cooling requirements
        """
        # Heat generation from losses
        i_squared_r_losses = (self.current ** 2) * self.internal_resistance / 1000  # kW
        
        # Heat from power electronics
        power_electronics_losses = abs(power_level) * 0.02  # 2% loss in inverter
        
        # Total heat generation
        total_heat = i_squared_r_losses + power_electronics_losses
        
        # Temperature rise
        thermal_mass = self.capacity_total * 3.6  # MJ/°C (approximation)
        temp_rise_rate = total_heat / thermal_mass if thermal_mass > 0 else 0
        
        # Natural cooling
        natural_cooling_rate = (self.battery_temperature - ambient_temp) * 0.1
        
        # Active cooling power requirement
        target_temp = 30.0  # °C
        if self.battery_temperature > target_temp:
            cooling_power = (self.battery_temperature - target_temp) * 10  # kW
        else:
            cooling_power = 0.0
        
        # Update battery temperature
        net_temp_change = temp_rise_rate - natural_cooling_rate - (cooling_power / thermal_mass)
        
        # Calculate thermal runaway risk
        thermal_runaway_risk = max(0, (self.battery_temperature - 60) / 40)  # Risk increases above 60°C
        
        return {
            "temperature_change": net_temp_change,
            "cooling_power": cooling_power,
            "thermal_runaway_risk": thermal_runaway_risk
        }
    
    def update_electrical_parameters(self, power_demand: float):
        """
        Update electrical parameters based on power demand
        """
        if power_demand == 0:
            self.current = 0.0
            self.voltage = 800.0  # Nominal voltage
            return
        
        # Calculate current (I = P / V)
        # Account for voltage drop due to internal resistance
        if power_demand > 0:  # Charging
            self.current = power_demand * 1000 / self.voltage  # A
            self.voltage = 800.0 + self.current * self.internal_resistance
        else:  # Discharging
            self.current = -power_demand * 1000 / self.voltage  # A (negative for discharge)
            self.voltage = 800.0 - abs(self.current) * self.internal_resistance
        
        # Voltage limits
        self.voltage = max(600, min(900, self.voltage))
    
    def check_safety_conditions(self):
        """
        Check battery safety conditions
        """
        # Temperature safety
        if self.battery_temperature > 50:
            self.safety_status = "warning"
        elif self.battery_temperature > 60:
            self.safety_status = "critical"
        else:
            self.safety_status = "normal"
        
        # Voltage safety
        if self.voltage < 650 or self.voltage > 850:
            self.safety_status = "warning"
        
        # SOC safety
        if self.state_of_charge < 0.05 or self.state_of_charge > 0.95:
            self.safety_status = "warning"
        
        # Simulate gas detection (very rare)
        if self.rng.random() < 0.0001:
            self.gas_detection_status = True
            self.safety_status = "critical"
        else:
            self.gas_detection_status = False
    
    def check_maintenance_requirements(self):
        """
        Check if maintenance is required
        """
        # Cell balancing required
        if self.cycle_count > 0 and self.cycle_count % 100 == 0:
            self.balancing_required = True
            self.maintenance_due = MaintenanceType.LIGHT
        
        # State of health check
        if self.state_of_health < 0.8:
            self.maintenance_due = MaintenanceType.MAJOR
        
        # Regular maintenance every year
        if self.hours_since_maintenance >= 8760:
            self.maintenance_due = MaintenanceType.LIGHT
        
        # Safety system check
        if self.safety_status == "critical":
            self.maintenance_due = MaintenanceType.EMERGENCY
    
    def perform_maintenance(self, maintenance_type: MaintenanceType):
        """
        Perform maintenance and update equipment state
        """
        if maintenance_type == MaintenanceType.LIGHT:
            self.hours_since_maintenance = 0.0
            self.balancing_required = False
            # Slight improvement in efficiency
            self.charging_efficiency = min(self.config.battery_efficiency, 
                                         self.charging_efficiency + 0.01)
            self.discharging_efficiency = min(self.config.battery_efficiency, 
                                            self.discharging_efficiency + 0.01)
        
        elif maintenance_type == MaintenanceType.MAJOR:
            self.hours_since_maintenance = 0.0
            self.balancing_required = False
            # Significant restoration (partial battery replacement)
            self.capacity_fade = max(0, self.capacity_fade - 0.1)
            self.state_of_health = 1.0 - self.capacity_fade
            self.capacity_available = self.capacity_total * self.state_of_health
            self.internal_resistance = 0.01 + self.capacity_fade * 0.05
        
        elif maintenance_type == MaintenanceType.EMERGENCY:
            self.hours_since_maintenance = 0.0
            self.safety_status = "normal"
            self.fire_detection_status = False
            self.gas_detection_status = False
        
        self.maintenance_due = MaintenanceType.NONE
    
    def set_power_demand(self, power_demand: float, ambient_temp: float = 25.0):
        """
        Set power demand (positive = charging, negative = discharging)
        """
        # Check SOC limits
        if power_demand > 0 and self.state_of_charge >= 0.95:
            power_demand = 0.0  # Stop charging at 95%
        elif power_demand < 0 and self.state_of_charge <= 0.05:
            power_demand = 0.0  # Stop discharging at 5%
        
        # Apply power limits
        max_power = self.capacity_total * 0.5  # C/2 rate limit
        power_demand = max(-max_power, min(max_power, power_demand))
        
        # Set power flows
        if power_demand > 0:
            self.power_input = power_demand
            self.power_output = 0.0
            self.status = EquipmentStatus.RUNNING
        elif power_demand < 0:
            self.power_input = 0.0
            self.power_output = -power_demand
            self.status = EquipmentStatus.RUNNING
        else:
            self.power_input = 0.0
            self.power_output = 0.0
            self.status = EquipmentStatus.IDLE
        
        # Update electrical parameters
        self.update_electrical_parameters(power_demand)
        
        # Calculate thermal behavior
        thermal_data = self.calculate_thermal_behavior(power_demand, ambient_temp)
        self.battery_temperature += thermal_data["temperature_change"] * 0.1  # Slow thermal response
        self.cooling_system_power = thermal_data["cooling_power"]
        self.thermal_runaway_risk = thermal_data["thermal_runaway_risk"]
    
    def update_state(self, dt_hours: float):
        """
        Update battery state for time step
        """
        # Update operating hours
        if self.status == EquipmentStatus.RUNNING:
            self.operating_hours += dt_hours
            self.hours_since_maintenance += dt_hours
        
        # Update SOC
        self.last_soc = self.state_of_charge
        
        if self.power_input > 0:
            # Charging
            energy_in = self.power_input * dt_hours * self.charging_efficiency
            self.state_of_charge += energy_in / self.capacity_available
        elif self.power_output > 0:
            # Discharging
            energy_out = self.power_output * dt_hours / self.discharging_efficiency
            self.state_of_charge -= energy_out / self.capacity_available
        
        # SOC limits
        self.state_of_charge = max(0.0, min(1.0, self.state_of_charge))
        
        # Update cycle count
        soc_change = abs(self.state_of_charge - self.last_soc)
        self.charging_cycles += soc_change * 0.5  # Half cycle for each direction
        if self.charging_cycles >= 1.0:
            self.cycle_count += int(self.charging_cycles)
            self.charging_cycles = self.charging_cycles % 1.0
        
        # Update degradation
        power_level = self.power_input if self.power_input > 0 else self.power_output
        self.update_degradation(power_level, self.battery_temperature, dt_hours)
        
        # Update round trip efficiency
        self.round_trip_efficiency = self.charging_efficiency * self.discharging_efficiency
        
        # Check safety conditions
        self.check_safety_conditions()
        
        # Check maintenance requirements
        self.check_maintenance_requirements()
    
    def generate_telemetry_data(self, timestamp: Optional[datetime] = None) -> BatteryStorageData:
        """
        Generate complete telemetry data point
        """
        if timestamp is None:
            timestamp = self.current_time
        
        return BatteryStorageData(
            timestamp=timestamp,
            equipment_id=self.equipment_id,
            location_id=self.location_id,
            
            status=self.status,
            operating_hours=self.operating_hours,
            
            state_of_charge=self.state_of_charge,
            capacity_available=self.capacity_available,
            capacity_total=self.capacity_total,
            power_input=self.power_input,
            power_output=self.power_output,
            
            charging_efficiency=self.charging_efficiency,
            discharging_efficiency=self.discharging_efficiency,
            round_trip_efficiency=self.round_trip_efficiency,
            
            state_of_health=self.state_of_health,
            cycle_count=self.cycle_count,
            calendar_age=self.calendar_age,
            capacity_fade=self.capacity_fade,
            
            battery_temperature=self.battery_temperature,
            cooling_system_power=self.cooling_system_power,
            thermal_runaway_risk=self.thermal_runaway_risk,
            
            voltage=self.voltage,
            current=self.current,
            internal_resistance=self.internal_resistance,
            
            safety_status=self.safety_status,
            fire_detection_status=self.fire_detection_status,
            gas_detection_status=self.gas_detection_status,
            
            maintenance_due=self.maintenance_due,
            balancing_required=self.balancing_required
        )
    
    def step(self, power_demand: float, ambient_temp: float = 25.0, dt_hours: float = 1/3600):
        """
        Single simulation step
        """
        # Set power demand
        self.set_power_demand(power_demand, ambient_temp)
        
        # Update state
        self.update_state(dt_hours)
        
        # Update time
        self.current_time += timedelta(hours=dt_hours)
        
        # Generate telemetry
        return self.generate_telemetry_data()
    
    def reset(self):
        """
        Reset equipment to initial state
        """
        self.status = EquipmentStatus.IDLE
        self.operating_hours = 0.0
        self.state_of_charge = 0.5
        self.capacity_available = self.capacity_total
        self.state_of_health = 1.0
        self.cycle_count = 0
        self.capacity_fade = 0.0
        self.charging_efficiency = self.config.battery_efficiency
        self.discharging_efficiency = self.config.battery_efficiency
        self.battery_temperature = 25.0
        self.safety_status = "normal"
        self.maintenance_due = MaintenanceType.NONE
        self.current_time = datetime.now()


class H2StorageSimulator:
    """
    Hydrogen storage system simulator with pressure vessel dynamics,
    compression, and safety considerations
    """
    
    def __init__(self, equipment_id: str, config=None, location_id: str = "plant_001"):
        self.equipment_id = equipment_id
        self.location_id = location_id
        self.config = config or EQUIPMENT_CONFIG
        self.current_time = datetime.now()
        
        # Initialize equipment state
        self.status = EquipmentStatus.IDLE
        self.operating_hours = 0.0
        self.hours_since_maintenance = 0.0
        
        # Storage state
        self.h2_mass_stored = 0.0  # kg
        self.storage_capacity = self.config.h2_storage_capacity  # kg
        self.fill_level = 0.0  # 0-1
        
        # Pressure and temperature
        self.pressure = 1.0  # bar (atmospheric)
        self.temperature = 25.0  # °C
        self.max_pressure = 350.0  # bar (design pressure)
        
        # Flow rates
        self.h2_input_rate = 0.0  # kg/h
        self.h2_output_rate = 0.0  # kg/h
        
        # Performance metrics
        self.compression_efficiency = 0.8
        self.storage_efficiency = 0.98  # Accounting for losses
        self.leak_rate = 0.001  # kg/h (very small)
        
        # Safety metrics
        self.pressure_safety_margin = 0.0  # bar
        self.gas_detection_level = 0.0  # ppm H2 in air
        self.ventilation_status = True
        self.emergency_vent_status = False
        
        # System health
        self.compressor_condition = 1.0  # 0-1
        self.tank_integrity = 1.0  # 0-1
        self.valve_condition = 1.0  # 0-1
        
        # Maintenance indicators
        self.maintenance_due = MaintenanceType.NONE
        self.pressure_test_due = False
        self.valve_inspection_due = False
        
        # Environmental conditions
        self.ambient_temperature = 25.0  # °C
        self.humidity = 50.0  # %
        
        # Random number generator
        self.rng = np.random.RandomState()
    
    def calculate_pressure_from_mass(self, mass: float, temp: float) -> float:
        """
        Calculate pressure from stored mass using ideal gas law
        """
        if mass <= 0:
            return 1.0  # Atmospheric pressure
        
        # Ideal gas law: PV = nRT
        # P = (m/M) * R * T / V
        # Where: m = mass (kg), M = molar mass (kg/mol), R = gas constant, T = temperature (K), V = volume (m³)
        
        molar_mass_h2 = 0.002  # kg/mol
        gas_constant = 8.314  # J/(mol·K)
        tank_volume = 100.0  # m³ (estimated for 1000 kg capacity)
        
        temp_kelvin = temp + 273.15
        pressure_pa = (mass / molar_mass_h2) * gas_constant * temp_kelvin / tank_volume
        pressure_bar = pressure_pa / 100000.0  # Convert Pa to bar
        
        return pressure_bar
    
    def calculate_compression_power(self, input_rate: float, target_pressure: float) -> float:
        """
        Calculate compression power required
        """
        if input_rate <= 0:
            return 0.0
        
        # Compression work (adiabatic compression)
        # W = (gamma/(gamma-1)) * P1 * V1 * [(P2/P1)^((gamma-1)/gamma) - 1]
        gamma = 1.4  # Heat capacity ratio for H2
        p1 = 1.0  # bar (atmospheric)
        p2 = target_pressure  # bar
        
        if p2 <= p1:
            return 0.0
        
        # Specific work (kJ/kg)
        specific_work = (gamma / (gamma - 1)) * p1 * 100 * ((p2 / p1) ** ((gamma - 1) / gamma) - 1)
        specific_work = specific_work / 1000.0  # Convert to kJ/kg
        
        # Total power (kW)
        power_kw = (input_rate * specific_work) / (3.6 * self.compression_efficiency)
        
        return power_kw
    
    def update_compressor_degradation(self, operating_hours: float, dt_hours: float):
        """
        Update compressor degradation
        """
        # Base degradation rate
        base_degradation = 0.0001  # per hour
        
        # Operating hours effect
        operating_factor = 1.0 + (operating_hours / 30000) * 0.3
        
        # Apply degradation
        degradation_rate = base_degradation * operating_factor
        self.compressor_condition = max(0.5, self.compressor_condition - degradation_rate * dt_hours)
        
        # Update compression efficiency
        self.compression_efficiency = 0.8 * self.compressor_condition
    
    def update_tank_integrity(self, pressure: float, dt_hours: float):
        """
        Update tank integrity based on pressure cycles
        """
        # Fatigue from pressure cycling
        if pressure > 100:  # Only significant at high pressures
            fatigue_rate = 0.000001 * (pressure / 350) ** 2
            self.tank_integrity = max(0.8, self.tank_integrity - fatigue_rate * dt_hours)
    
    def calculate_leak_rate(self) -> float:
        """
        Calculate hydrogen leak rate based on system condition
        """
        # Base leak rate
        base_leak = 0.001  # kg/h
        
        # Pressure effect
        pressure_factor = self.pressure / 350.0
        
        # System condition effect
        condition_factor = 2.0 - (self.tank_integrity * self.valve_condition)
        
        # Total leak rate
        leak_rate = base_leak * pressure_factor * condition_factor
        
        return leak_rate
    
    def check_safety_conditions(self):
        """
        Check hydrogen safety conditions
        """
        # Pressure safety margin
        self.pressure_safety_margin = self.max_pressure - self.pressure
        
        # Gas detection simulation
        if self.leak_rate > 0.01:  # Significant leak
            self.gas_detection_level = min(1000, self.leak_rate * 100)  # ppm
        else:
            self.gas_detection_level = max(0, self.gas_detection_level - 10)  # Natural dissipation
        
        # Emergency venting
        if self.pressure > 320:  # 320 bar trigger
            self.emergency_vent_status = True
        elif self.pressure < 300:
            self.emergency_vent_status = False
        
        # Ventilation status
        if self.gas_detection_level > 1000:  # 1000 ppm (25% of LEL)
            self.ventilation_status = True
        else:
            self.ventilation_status = self.rng.random() < 0.95  # 95% uptime
    
    def check_maintenance_requirements(self):
        """
        Check if maintenance is required
        """
        # Pressure test (every 2 years)
        if self.operating_hours >= 17520:  # 2 years
            self.pressure_test_due = True
            self.maintenance_due = MaintenanceType.MAJOR
        
        # Valve inspection (every year)
        if self.operating_hours >= 8760:  # 1 year
            self.valve_inspection_due = True
            self.maintenance_due = MaintenanceType.LIGHT
        
        # Compressor condition
        if self.compressor_condition < 0.7:
            self.maintenance_due = MaintenanceType.MAJOR
        
        # Tank integrity
        if self.tank_integrity < 0.9:
            self.maintenance_due = MaintenanceType.MAJOR
        
        # Safety system checks
        if self.gas_detection_level > 2000:  # 2000 ppm (critical)
            self.maintenance_due = MaintenanceType.EMERGENCY
    
    def perform_maintenance(self, maintenance_type: MaintenanceType):
        """
        Perform maintenance and update equipment state
        """
        if maintenance_type == MaintenanceType.LIGHT:
            self.hours_since_maintenance = 0.0
            self.valve_inspection_due = False
            self.valve_condition = min(1.0, self.valve_condition + 0.1)
        
        elif maintenance_type == MaintenanceType.MAJOR:
            self.hours_since_maintenance = 0.0
            self.pressure_test_due = False
            self.valve_inspection_due = False
            # Compressor overhaul
            self.compressor_condition = min(1.0, self.compressor_condition + 0.2)
            self.compression_efficiency = 0.8 * self.compressor_condition
            # Tank inspection
            self.tank_integrity = min(1.0, self.tank_integrity + 0.05)
            self.valve_condition = 1.0
        
        elif maintenance_type == MaintenanceType.EMERGENCY:
            self.hours_since_maintenance = 0.0
            self.gas_detection_level = 0.0
            self.emergency_vent_status = False
        
        self.maintenance_due = MaintenanceType.NONE
    
    def set_flow_rates(self, h2_input: float, h2_output: float, ambient_temp: float = 25.0):
        """
        Set hydrogen input and output rates
        """
        self.h2_input_rate = max(0, h2_input)
        self.h2_output_rate = max(0, h2_output)
        self.ambient_temperature = ambient_temp
        
        # Update status
        if self.h2_input_rate > 0 or self.h2_output_rate > 0:
            self.status = EquipmentStatus.RUNNING
        else:
            self.status = EquipmentStatus.IDLE
    
    def update_state(self, dt_hours: float):
        """
        Update storage state for time step
        """
        # Update operating hours
        if self.status == EquipmentStatus.RUNNING:
            self.operating_hours += dt_hours
            self.hours_since_maintenance += dt_hours
        
        # Update stored mass
        net_flow = self.h2_input_rate - self.h2_output_rate
        self.leak_rate = self.calculate_leak_rate()
        net_flow -= self.leak_rate
        
        # Emergency venting
        if self.emergency_vent_status:
            vent_rate = min(50, self.h2_mass_stored / dt_hours)  # Emergency vent rate
            net_flow -= vent_rate
        
        # Update mass
        self.h2_mass_stored += net_flow * dt_hours
        self.h2_mass_stored = max(0, min(self.storage_capacity, self.h2_mass_stored))
        
        # Update fill level
        self.fill_level = self.h2_mass_stored / self.storage_capacity
        
        # Update pressure and temperature
        self.pressure = self.calculate_pressure_from_mass(self.h2_mass_stored, self.temperature)
        
        # Temperature follows ambient with some thermal mass
        temp_change = (self.ambient_temperature - self.temperature) * 0.1
        self.temperature += temp_change * dt_hours
        
        # Update component degradation
        self.update_compressor_degradation(self.operating_hours, dt_hours)
        self.update_tank_integrity(self.pressure, dt_hours)
        
        # Check safety conditions
        self.check_safety_conditions()
        
        # Check maintenance requirements
        self.check_maintenance_requirements()
    
    def generate_telemetry_data(self, timestamp: Optional[datetime] = None) -> H2StorageData:
        """
        Generate complete telemetry data point
        """
        if timestamp is None:
            timestamp = self.current_time
        
        return H2StorageData(
            timestamp=timestamp,
            equipment_id=self.equipment_id,
            location_id=self.location_id,
            
            status=self.status,
            operating_hours=self.operating_hours,
            
            h2_mass_stored=self.h2_mass_stored,
            storage_capacity=self.storage_capacity,
            fill_level=self.fill_level,
            pressure=self.pressure,
            temperature=self.temperature,
            
            h2_input_rate=self.h2_input_rate,
            h2_output_rate=self.h2_output_rate,
            
            compression_efficiency=self.compression_efficiency,
            storage_efficiency=self.storage_efficiency,
            leak_rate=self.leak_rate,
            
            pressure_safety_margin=self.pressure_safety_margin,
            gas_detection_level=self.gas_detection_level,
            ventilation_status=self.ventilation_status,
            emergency_vent_status=self.emergency_vent_status,
            
            compressor_condition=self.compressor_condition,
            tank_integrity=self.tank_integrity,
            valve_condition=self.valve_condition,
            
            maintenance_due=self.maintenance_due,
            pressure_test_due=self.pressure_test_due,
            valve_inspection_due=self.valve_inspection_due,
            
            ambient_temperature=self.ambient_temperature,
            humidity=self.humidity
        )
    
    def step(self, h2_input: float, h2_output: float, ambient_temp: float = 25.0, 
             dt_hours: float = 1/3600):
        """
        Single simulation step
        """
        # Set flow rates
        self.set_flow_rates(h2_input, h2_output, ambient_temp)
        
        # Update state
        self.update_state(dt_hours)
        
        # Update time
        self.current_time += timedelta(hours=dt_hours)
        
        # Generate telemetry
        return self.generate_telemetry_data()
    
    def reset(self):
        """
        Reset equipment to initial state
        """
        self.status = EquipmentStatus.IDLE
        self.operating_hours = 0.0
        self.h2_mass_stored = 0.0
        self.fill_level = 0.0
        self.pressure = 1.0
        self.temperature = 25.0
        self.compressor_condition = 1.0
        self.tank_integrity = 1.0
        self.valve_condition = 1.0
        self.compression_efficiency = 0.8
        self.leak_rate = 0.001
        self.maintenance_due = MaintenanceType.NONE
        self.pressure_test_due = False
        self.valve_inspection_due = False
        self.current_time = datetime.now()