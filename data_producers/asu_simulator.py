

import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math
import random

from config.data_config import EQUIPMENT_CONFIG
from schemas.equipment_schema import ASUData, EquipmentStatus, MaintenanceType

class ASUSimulator:
    """
    Air Separation Unit simulator with realistic distillation column behavior,
    compressor performance, and filter/molecular sieve degradation
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
        
        # Performance state variables
        self.compressor_efficiency = 0.85  # Initial compressor efficiency
        self.heat_exchanger_effectiveness = 0.92  # Initial heat exchanger effectiveness
        self.molecular_sieve_condition = 1.0  # 1 = new, 0 = needs replacement
        self.filter_pressure_drop = 0.1  # Initial pressure drop (bar)
        
        # Production state
        self.n2_production_rate = 0.0
        self.o2_production_rate = 0.0
        self.n2_purity = self.config.asu_purity_nominal
        self.o2_purity = 95.0  # % O2 purity
        
        # Power consumption breakdown
        self.compressor_power = 0.0
        self.refrigeration_power = 0.0
        self.auxiliary_power = 0.0
        
        # Maintenance indicators
        self.maintenance_due = MaintenanceType.NONE
        self.next_maintenance_hours = 8760.0  # 1 year
        self.filter_replacement_due = False
        self.molecular_sieve_regeneration_due = False
        
        # Process parameters
        self.distillation_column_pressure = 5.5  # bar
        self.inlet_air_pressure = 1.013  # bar
        self.inlet_air_temperature = 25.0  # °C
        
        # Random number generator
        self.rng = np.random.RandomState()
        
        # Operating parameters
        self.air_intake_flow = 0.0
        self.compressor_stages = 3
        self.production_efficiency = 0.85
    
    def update_compressor_degradation(self, load_factor: float, dt_hours: float):
        """
        Update compressor degradation based on operating conditions
        """
        # Base degradation rate (per hour)
        base_degradation = 0.0001
        
        # Degradation factors
        load_factor_effect = 1.0 + (load_factor - 0.7) * 0.3  # Optimal around 70%
        operating_hours_effect = 1.0 + (self.operating_hours / 50000) * 0.2  # Wear over time
        
        # Apply degradation
        degradation_rate = base_degradation * load_factor_effect * operating_hours_effect
        self.compressor_efficiency = max(0.6, self.compressor_efficiency - degradation_rate * dt_hours)
    
    def update_filter_condition(self, air_flow: float, dt_hours: float):
        """
        Update filter condition based on air flow and operating time
        """
        # Filter degradation increases pressure drop
        base_degradation = 0.0001  # bar per hour
        flow_effect = (air_flow / 1000) * 0.5  # Higher flow = more degradation
        
        self.filter_pressure_drop += base_degradation * flow_effect * dt_hours
        
        # Filter replacement needed if pressure drop > 0.8 bar
        if self.filter_pressure_drop > 0.8:
            self.filter_replacement_due = True
    
    def update_molecular_sieve_condition(self, dt_hours: float):
        """
        Update molecular sieve condition (used for moisture removal)
        """
        # Molecular sieve degrades slowly over time
        base_degradation = 0.00005  # per hour
        
        self.molecular_sieve_condition = max(0.3, self.molecular_sieve_condition - base_degradation * dt_hours)
        
        # Regeneration needed if condition < 0.7
        if self.molecular_sieve_condition < 0.7:
            self.molecular_sieve_regeneration_due = True
    
    def update_heat_exchanger_effectiveness(self, dt_hours: float):
        """
        Update heat exchanger effectiveness (fouling over time)
        """
        # Heat exchanger fouling reduces effectiveness
        base_degradation = 0.00002  # per hour
        
        self.heat_exchanger_effectiveness = max(0.7, self.heat_exchanger_effectiveness - base_degradation * dt_hours)
    
    def calculate_production_rates(self, demand_n2: float, demand_o2: float) -> Tuple[float, float, float]:
        """
        Calculate N2 and O2 production rates based on demand and efficiency
        """
        # Air composition: ~78% N2, ~21% O2, ~1% other
        n2_fraction = 0.78
        o2_fraction = 0.21
        
        # Calculate required air flow based on demand
        air_flow_for_n2 = demand_n2 / n2_fraction if n2_fraction > 0 else 0
        air_flow_for_o2 = demand_o2 / o2_fraction if o2_fraction > 0 else 0
        
        # Total air flow needed
        total_air_flow = max(air_flow_for_n2, air_flow_for_o2)
        
        # Apply efficiency losses
        actual_efficiency = self.production_efficiency * self.compressor_efficiency * self.heat_exchanger_effectiveness
        required_air_flow = total_air_flow / actual_efficiency
        
        # Calculate actual production rates
        n2_rate = required_air_flow * n2_fraction * actual_efficiency
        o2_rate = required_air_flow * o2_fraction * actual_efficiency
        
        return n2_rate, o2_rate, required_air_flow
    
    def calculate_power_consumption(self, air_flow: float, n2_rate: float, o2_rate: float) -> Dict[str, float]:
        """
        Calculate power consumption for different components
        """
        if air_flow <= 0:
            return {"compressor": 0.0, "refrigeration": 0.0, "auxiliary": 0.0}
        
        # Compressor power (multiple stages)
        # Power = (air_flow * pressure_ratio * efficiency_factor)
        compression_ratio = self.distillation_column_pressure / self.inlet_air_pressure
        compressor_power = (air_flow / 1000) * math.log(compression_ratio) * 0.3 / self.compressor_efficiency
        
        # Refrigeration power for distillation column
        refrigeration_power = (n2_rate + o2_rate) * 0.0008  # kW per Nm³/h
        
        # Auxiliary power (controls, pumps, etc.)
        auxiliary_power = 0.1 * (compressor_power + refrigeration_power)
        
        # Apply filter pressure drop penalty
        pressure_drop_penalty = self.filter_pressure_drop * 0.1
        compressor_power *= (1.0 + pressure_drop_penalty)
        
        return {
            "compressor": compressor_power,
            "refrigeration": refrigeration_power,
            "auxiliary": auxiliary_power
        }
    
    def calculate_product_purity(self, production_rate: float, max_rate: float) -> Dict[str, float]:
        """
        Calculate product purity based on production rate and equipment condition
        """
        # Load factor affects purity
        load_factor = production_rate / max_rate if max_rate > 0 else 0
        
        # Optimal purity at moderate load (70-80%)
        purity_factor = 1.0 - abs(load_factor - 0.75) * 0.3
        
        # Equipment condition affects purity
        condition_factor = (self.compressor_efficiency * self.heat_exchanger_effectiveness * 
                          self.molecular_sieve_condition)
        
        # Calculate N2 and O2 purity
        n2_purity = self.config.asu_purity_nominal * purity_factor * condition_factor
        o2_purity = 95.0 * purity_factor * condition_factor
        
        # Add some realistic noise
        n2_purity += self.rng.normal(0, 0.05)
        o2_purity += self.rng.normal(0, 0.1)
        
        # Ensure realistic bounds
        n2_purity = max(95.0, min(99.99, n2_purity))
        o2_purity = max(90.0, min(99.0, o2_purity))
        
        return {"n2_purity": n2_purity, "o2_purity": o2_purity}
    
    def check_maintenance_requirements(self):
        """
        Check if maintenance is required
        """
        # Check filter replacement
        if self.filter_replacement_due:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Check molecular sieve regeneration
        if self.molecular_sieve_regeneration_due:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Check compressor efficiency
        if self.compressor_efficiency < 0.75:
            self.maintenance_due = MaintenanceType.MAJOR
            return
        
        # Check heat exchanger effectiveness
        if self.heat_exchanger_effectiveness < 0.8:
            self.maintenance_due = MaintenanceType.MAJOR
            return
        
        # Regular maintenance schedule
        if self.hours_since_maintenance >= 4380:  # 6 months
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Major maintenance every 2 years
        if self.operating_hours >= self.next_maintenance_hours:
            self.maintenance_due = MaintenanceType.MAJOR
            return
        
        self.maintenance_due = MaintenanceType.NONE
    
    def perform_maintenance(self, maintenance_type: MaintenanceType):
        """
        Perform maintenance and update equipment state
        """
        if maintenance_type == MaintenanceType.LIGHT:
            # Light maintenance - filter replacement, molecular sieve regeneration
            self.hours_since_maintenance = 0.0
            
            if self.filter_replacement_due:
                self.filter_pressure_drop = 0.1
                self.filter_replacement_due = False
            
            if self.molecular_sieve_regeneration_due:
                self.molecular_sieve_condition = 1.0
                self.molecular_sieve_regeneration_due = False
            
        elif maintenance_type == MaintenanceType.MAJOR:
            # Major maintenance - compressor overhaul, heat exchanger cleaning
            self.hours_since_maintenance = 0.0
            self.next_maintenance_hours = self.operating_hours + 17520  # 2 years
            
            # Restore component efficiencies
            self.compressor_efficiency = min(0.85, self.compressor_efficiency + 0.1)
            self.heat_exchanger_effectiveness = min(0.92, self.heat_exchanger_effectiveness + 0.1)
            self.filter_pressure_drop = 0.1
            self.molecular_sieve_condition = 1.0
            self.filter_replacement_due = False
            self.molecular_sieve_regeneration_due = False
        
        self.maintenance_due = MaintenanceType.NONE
    
    def set_production_demand(self, n2_demand: float, o2_demand: float = 0.0):
        """
        Set production demand and update equipment state
        """
        if n2_demand > 0 and self.status == EquipmentStatus.IDLE:
            self.status = EquipmentStatus.RUNNING
        elif n2_demand <= 0 and self.status == EquipmentStatus.RUNNING:
            self.status = EquipmentStatus.IDLE
        
        # Calculate production rates
        n2_rate, o2_rate, air_flow = self.calculate_production_rates(n2_demand, o2_demand)
        
        # Update state variables
        self.n2_production_rate = min(n2_rate, self.config.asu_rated_capacity)
        self.o2_production_rate = o2_rate
        self.air_intake_flow = air_flow
        
        # Calculate power consumption
        power_breakdown = self.calculate_power_consumption(air_flow, n2_rate, o2_rate)
        self.compressor_power = power_breakdown["compressor"]
        self.refrigeration_power = power_breakdown["refrigeration"]
        self.auxiliary_power = power_breakdown["auxiliary"]
        
        # Calculate product purity
        purity_data = self.calculate_product_purity(n2_rate, self.config.asu_rated_capacity)
        self.n2_purity = purity_data["n2_purity"]
        self.o2_purity = purity_data["o2_purity"]
    
    def update_state(self, dt_hours: float):
        """
        Update equipment state for time step
        """
        if self.status == EquipmentStatus.RUNNING:
            # Update operating hours
            self.operating_hours += dt_hours
            self.hours_since_maintenance += dt_hours
            
            # Update component degradation
            load_factor = self.air_intake_flow / 1000  # Normalize to typical max flow
            self.update_compressor_degradation(load_factor, dt_hours)
            self.update_filter_condition(self.air_intake_flow, dt_hours)
            self.update_molecular_sieve_condition(dt_hours)
            self.update_heat_exchanger_effectiveness(dt_hours)
        
        # Check maintenance requirements
        self.check_maintenance_requirements()
        
        # Update status based on maintenance needs
        if self.maintenance_due == MaintenanceType.MAJOR:
            # Major maintenance may require shutdown
            pass
    
    def generate_telemetry_data(self, timestamp: Optional[datetime] = None) -> ASUData:
        """
        Generate complete telemetry data point
        """
        if timestamp is None:
            timestamp = self.current_time
        
        # Calculate total power consumption
        total_power = (self.compressor_power + self.refrigeration_power + self.auxiliary_power) / 1000  # MW
        
        # Calculate production efficiency
        theoretical_power = self.air_intake_flow * self.config.asu_power_consumption / 1000  # MW
        production_efficiency = min(1.0, theoretical_power / total_power if total_power > 0 else 0)
        
        # Calculate quality metrics
        product_quality_score = min(1.0, (self.n2_purity / self.config.asu_purity_nominal + 
                                        self.o2_purity / 95.0) / 2.0)
        
        # Process stability (based on equipment condition)
        process_stability = (self.compressor_efficiency * self.heat_exchanger_effectiveness * 
                           self.molecular_sieve_condition)
        
        # Generate environmental conditions
        cooling_water_temp = 25.0 + self.rng.normal(0, 3)
        
        return ASUData(
            timestamp=timestamp,
            equipment_id=self.equipment_id,
            location_id=self.location_id,
            
            status=self.status,
            operating_hours=self.operating_hours,
            
            power_consumption=total_power,
            n2_production_rate=self.n2_production_rate,
            o2_production_rate=self.o2_production_rate,
            production_efficiency=production_efficiency,
            
            n2_purity=self.n2_purity,
            o2_purity=self.o2_purity,
            air_intake_flow=self.air_intake_flow,
            compressor_stages=self.compressor_stages,
            
            compressor_efficiency=self.compressor_efficiency,
            heat_exchanger_effectiveness=self.heat_exchanger_effectiveness,
            molecular_sieve_condition=self.molecular_sieve_condition,
            filter_pressure_drop=self.filter_pressure_drop,
            
            inlet_air_pressure=self.inlet_air_pressure,
            inlet_air_temperature=self.inlet_air_temperature,
            distillation_column_pressure=self.distillation_column_pressure,
            cooling_water_temperature=cooling_water_temp,
            
            compressor_power=self.compressor_power / 1000,  # MW
            refrigeration_power=self.refrigeration_power / 1000,  # MW
            auxiliary_power=self.auxiliary_power / 1000,  # MW
            
            maintenance_due=self.maintenance_due,
            hours_since_maintenance=self.hours_since_maintenance,
            filter_replacement_due=self.filter_replacement_due,
            molecular_sieve_regeneration_due=self.molecular_sieve_regeneration_due,
            
            product_quality_score=product_quality_score,
            process_stability=process_stability
        )
    
    def step(self, n2_demand: float, o2_demand: float = 0.0, dt_hours: float = 1/3600):
        """
        Single simulation step
        """
        # Set production demand
        self.set_production_demand(n2_demand, o2_demand)
        
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
        self.hours_since_maintenance = 0.0
        self.compressor_efficiency = 0.85
        self.heat_exchanger_effectiveness = 0.92
        self.molecular_sieve_condition = 1.0
        self.filter_pressure_drop = 0.1
        self.n2_production_rate = 0.0
        self.o2_production_rate = 0.0
        self.maintenance_due = MaintenanceType.NONE
        self.filter_replacement_due = False
        self.molecular_sieve_regeneration_due = False
        self.current_time = datetime.now()