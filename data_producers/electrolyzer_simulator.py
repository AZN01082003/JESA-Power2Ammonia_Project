

import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math
import random

from config.data_config import EQUIPMENT_CONFIG
from schemas.equipment_schema import ElectrolyzerData, EquipmentStatus, MaintenanceType

class ElectrolyzerSimulator:
    """
    Advanced electrolyzer simulator with realistic degradation patterns,
    maintenance cycles, and operational constraints
    """
    
    def __init__(self, equipment_id: str, config=None, location_id: str = "plant_001"):
        self.equipment_id = equipment_id
        self.location_id = location_id
        self.config = config or EQUIPMENT_CONFIG
        self.current_time = datetime.now()
        
        # Initialize equipment state
        self.status = EquipmentStatus.IDLE
        self.operating_hours = 0.0
        self.cycles_count = 0
        self.hours_since_maintenance = 0.0
        
        # Performance state variables
        self.efficiency_current = self.config.electrolyzer_efficiency_nominal
        self.stack_degradation = 1.0  # 1 = new, 0 = end of life
        self.membrane_resistance = 0.1  # Initial resistance in Ohm
        self.stack_temperature = 25.0  # °C
        
        # Operational state
        self.current_power = 0.0
        self.load_factor = 0.0
        self.h2_production_cumulative = 0.0
        
        # Maintenance scheduling
        self.maintenance_due = MaintenanceType.NONE
        self.next_maintenance_hours = 8760.0  # 1 year
        self.fault_codes = []
        
        # Random number generator for reproducibility
        self.rng = np.random.RandomState()
        
        # Initialize stack parameters
        self.stack_voltage = 0.0
        self.stack_current = 0.0
        self.water_consumption = 0.0
        
    def update_degradation(self, power_level: float, temperature: float, dt_hours: float):
        """
        Update stack degradation based on operating conditions
        """
        # Base degradation rate (per hour)
        base_degradation = self.config.electrolyzer_degradation_rate
        
        # Degradation accelerators
        temp_factor = 1.0 + max(0, (temperature - 60) / 100)  # Higher temp = faster degradation
        load_factor = 1.0 + (power_level - 0.5) * 0.5  # High load = faster degradation
        cycle_factor = 1.0 + (self.cycles_count / 10000) * 0.1  # More cycles = faster degradation
        
        # Apply degradation
        degradation_rate = base_degradation * temp_factor * load_factor * cycle_factor
        self.stack_degradation = max(0.1, self.stack_degradation - degradation_rate * dt_hours)
        
        # Update membrane resistance (increases with degradation)
        self.membrane_resistance = 0.1 + (1.0 - self.stack_degradation) * 0.5
        
        # Update efficiency based on degradation
        efficiency_loss = (1.0 - self.stack_degradation) * 0.3  # Max 30% efficiency loss
        self.efficiency_current = max(0.4, self.config.electrolyzer_efficiency_nominal - efficiency_loss)
    
    def calculate_stack_parameters(self, power_demand: float, ambient_temp: float) -> Dict[str, float]:
        """
        Calculate stack voltage, current, and temperature
        """
        if power_demand <= 0:
            return {
                "voltage": 0.0,
                "current": 0.0,
                "temperature": ambient_temp
            }
        
        # Load factor
        load_factor = min(1.0, power_demand / self.config.electrolyzer_rated_power)
        load_factor = max(self.config.electrolyzer_min_load, load_factor)
        
        # Stack voltage (decreases with load due to losses)
        nominal_voltage = 800.0  # V (typical for MW-scale electrolyzer)
        voltage_drop = load_factor * 50.0 + self.membrane_resistance * 100.0
        stack_voltage = nominal_voltage - voltage_drop
        
        # Stack current (from P = V * I)
        actual_power = load_factor * self.config.electrolyzer_rated_power * 1000  # kW
        stack_current = actual_power / stack_voltage if stack_voltage > 0 else 0.0
        
        # Stack temperature (increases with load)
        temp_rise = load_factor * 40.0 + (1.0 - self.efficiency_current) * 20.0
        stack_temperature = ambient_temp + temp_rise
        
        return {
            "voltage": stack_voltage,
            "current": stack_current,
            "temperature": stack_temperature
        }
    
    def calculate_h2_production(self, power: float, efficiency: float) -> Tuple[float, float]:
        """
        Calculate H2 production rate and water consumption
        """
        if power <= 0:
            return 0.0, 0.0
        
        # H2 production (theoretical: 39.4 kWh/kg H2)
        theoretical_energy = 39.4  # kWh/kg H2
        actual_energy = theoretical_energy / efficiency
        h2_rate = (power * 1000) / actual_energy  # kg/h
        
        # Water consumption (theoretical: 9 kg H2O per kg H2)
        water_consumption = h2_rate * 9.0  # L/h (assuming 1 L = 1 kg for water)
        
        return h2_rate, water_consumption
    
    def check_maintenance_requirements(self):
        """
        Check if maintenance is required based on various indicators
        """
        # Check operating hours
        if self.operating_hours >= self.next_maintenance_hours:
            self.maintenance_due = MaintenanceType.MAJOR
            return
        
        # Check stack degradation
        if self.stack_degradation < 0.7:
            self.maintenance_due = MaintenanceType.MAJOR
            return
        
        # Check efficiency drop
        efficiency_drop = (self.config.electrolyzer_efficiency_nominal - self.efficiency_current) / self.config.electrolyzer_efficiency_nominal
        if efficiency_drop > 0.15:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Check membrane resistance
        if self.membrane_resistance > 0.4:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Check cycles count
        if self.cycles_count % 1000 == 0 and self.cycles_count > 0:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Light maintenance every 4380 hours (6 months)
        if self.hours_since_maintenance >= 4380:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        self.maintenance_due = MaintenanceType.NONE
    
    def simulate_faults(self) -> List[str]:
        """
        Simulate random faults based on equipment condition
        """
        fault_probability = (1.0 - self.stack_degradation) * 0.001  # Higher probability with degradation
        
        possible_faults = [
            "MEMBRANE_LEAK",
            "COOLING_SYSTEM_FAULT",
            "POWER_SUPPLY_INSTABILITY",
            "GAS_PURITY_LOW",
            "WATER_QUALITY_ISSUE",
            "PRESSURE_REGULATOR_FAULT",
            "TEMPERATURE_SENSOR_FAULT"
        ]
        
        active_faults = []
        for fault in possible_faults:
            if self.rng.random() < fault_probability:
                active_faults.append(fault)
        
        return active_faults
    
    def perform_maintenance(self, maintenance_type: MaintenanceType):
        """
        Perform maintenance and update equipment state
        """
        if maintenance_type == MaintenanceType.LIGHT:
            # Light maintenance
            self.hours_since_maintenance = 0.0
            self.next_maintenance_hours = self.operating_hours + 4380  # 6 months
            self.fault_codes.clear()
            
        elif maintenance_type == MaintenanceType.MAJOR:
            # Major maintenance - significant restoration
            self.hours_since_maintenance = 0.0
            self.next_maintenance_hours = self.operating_hours + 8760  # 1 year
            self.stack_degradation = min(1.0, self.stack_degradation + 0.3)  # Partial restoration
            self.membrane_resistance = max(0.1, self.membrane_resistance - 0.2)
            self.efficiency_current = min(self.config.electrolyzer_efficiency_nominal, 
                                        self.efficiency_current + 0.1)
            self.fault_codes.clear()
        
        self.maintenance_due = MaintenanceType.NONE
    
    def set_power_demand(self, power_demand: float, ambient_temp: float = 25.0):
        """
        Set power demand and update equipment state
        """
        # Check if power demand requires starting/stopping
        if power_demand > 0 and self.status == EquipmentStatus.IDLE:
            self.status = EquipmentStatus.STARTING
            self.cycles_count += 1
        elif power_demand <= 0 and self.status == EquipmentStatus.RUNNING:
            self.status = EquipmentStatus.STOPPING
        
        # Update power and load factor
        self.current_power = min(power_demand, self.config.electrolyzer_rated_power)
        self.load_factor = self.current_power / self.config.electrolyzer_rated_power
        
        # Check minimum load constraint
        if self.load_factor > 0 and self.load_factor < self.config.electrolyzer_min_load:
            self.current_power = self.config.electrolyzer_min_load * self.config.electrolyzer_rated_power
            self.load_factor = self.config.electrolyzer_min_load
        
        # Update stack parameters
        stack_params = self.calculate_stack_parameters(self.current_power, ambient_temp)
        self.stack_voltage = stack_params["voltage"]
        self.stack_current = stack_params["current"]
        self.stack_temperature = stack_params["temperature"]
        
        # Update status based on actual operation
        if self.current_power > 0:
            self.status = EquipmentStatus.RUNNING
        else:
            self.status = EquipmentStatus.IDLE
    
    def update_state(self, dt_hours: float):
        """
        Update equipment state for time step
        """
        # Update operating hours
        if self.status == EquipmentStatus.RUNNING:
            self.operating_hours += dt_hours
            self.hours_since_maintenance += dt_hours
            
            # Update degradation
            self.update_degradation(self.load_factor, self.stack_temperature, dt_hours)
            
            # Update cumulative production
            h2_rate, water_rate = self.calculate_h2_production(self.current_power, self.efficiency_current)
            self.h2_production_cumulative += h2_rate * dt_hours
            self.water_consumption = water_rate
        else:
            self.water_consumption = 0.0
        
        # Check maintenance requirements
        self.check_maintenance_requirements()
        
        # Simulate faults
        self.fault_codes = self.simulate_faults()
        
        # Handle maintenance status
        if self.fault_codes:
            self.status = EquipmentStatus.FAULT
        elif self.maintenance_due != MaintenanceType.NONE:
            # Don't automatically go to maintenance - wait for external command
            pass
    
    def generate_telemetry_data(self, timestamp: Optional[datetime] = None) -> ElectrolyzerData:
        """
        Generate complete telemetry data point
        """
        if timestamp is None:
            timestamp = self.current_time
        
        # Calculate current H2 production rate
        h2_rate, water_rate = self.calculate_h2_production(self.current_power, self.efficiency_current)
        
        # Calculate performance metrics
        availability = 1.0 if self.status in [EquipmentStatus.RUNNING, EquipmentStatus.IDLE] else 0.0
        reliability_score = self.stack_degradation * (1.0 - len(self.fault_codes) * 0.1)
        
        # Calculate specific energy consumption
        specific_energy = (39.4 / self.efficiency_current) if self.efficiency_current > 0 else 0.0
        
        # Generate some realistic environmental conditions
        ambient_temp = 25.0 + self.rng.normal(0, 5)  # °C
        cooling_water_temp = ambient_temp + 5.0 + self.rng.normal(0, 2)
        system_pressure = 30.0 + self.rng.normal(0, 2)  # bar
        
        # Gas purity (decreases with degradation)
        gas_purity = 99.9 - (1.0 - self.stack_degradation) * 0.5 + self.rng.normal(0, 0.1)
        
        return ElectrolyzerData(
            timestamp=timestamp,
            equipment_id=self.equipment_id,
            location_id=self.location_id,
            
            status=self.status,
            operating_hours=self.operating_hours,
            cycles_count=self.cycles_count,
            
            current_power=self.current_power,
            rated_power=self.config.electrolyzer_rated_power,
            load_factor=self.load_factor,
            efficiency=self.efficiency_current,
            efficiency_nominal=self.config.electrolyzer_efficiency_nominal,
            
            h2_production_rate=h2_rate,
            h2_production_cumulative=self.h2_production_cumulative,
            stack_voltage=self.stack_voltage,
            stack_current=self.stack_current,
            stack_temperature=self.stack_temperature,
            
            stack_degradation=self.stack_degradation,
            membrane_resistance=self.membrane_resistance,
            gas_purity_h2=gas_purity,
            water_consumption=self.water_consumption,
            
            maintenance_due=self.maintenance_due,
            hours_since_maintenance=self.hours_since_maintenance,
            next_maintenance_hours=self.next_maintenance_hours,
            fault_codes=self.fault_codes.copy(),
            
            ambient_temperature=ambient_temp,
            cooling_water_temp=cooling_water_temp,
            system_pressure=system_pressure,
            
            specific_energy_consumption=specific_energy,
            availability=availability,
            reliability_score=reliability_score
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
        self.cycles_count = 0
        self.hours_since_maintenance = 0.0
        self.efficiency_current = self.config.electrolyzer_efficiency_nominal
        self.stack_degradation = 1.0
        self.membrane_resistance = 0.1
        self.current_power = 0.0
        self.load_factor = 0.0
        self.h2_production_cumulative = 0.0
        self.maintenance_due = MaintenanceType.NONE
        self.next_maintenance_hours = 8760.0
        self.fault_codes.clear()
        self.current_time = datetime.now()