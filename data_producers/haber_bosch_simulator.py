import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math
import random

from config.data_config import EQUIPMENT_CONFIG
from schemas.equipment_schema import HaberBoschData, EquipmentStatus, MaintenanceType

class HaberBoschSimulator:
    """
    Haber-Bosch reactor simulator with realistic catalyst behavior,
    reaction kinetics, and thermodynamic constraints
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
        
        # Catalyst state
        self.catalyst_activity = 1.0  # 1 = fresh catalyst, 0 = deactivated
        self.catalyst_age = 0.0  # hours
        self.catalyst_replacement_due = False
        
        # Process parameters
        self.reactor_temperature = 450.0  # °C (optimal for Haber-Bosch)
        self.reactor_pressure = 200.0  # bar (typical industrial pressure)
        self.catalyst_bed_temperature = 450.0  # °C
        
        # Performance metrics
        self.conversion_efficiency = self.config.hb_conversion_efficiency
        self.selectivity = 0.98  # NH3 selectivity
        self.heat_recovery_efficiency = 0.8
        self.compression_efficiency = 0.85
        self.separation_efficiency = 0.95
        
        # Production state
        self.nh3_production_rate = 0.0  # t/h
        self.nh3_production_cumulative = 0.0  # t
        self.energy_consumption = 0.0  # MW
        
        # Reactant flows
        self.n2_feed_rate = 0.0  # kg/h
        self.h2_feed_rate = 0.0  # kg/h
        self.n2_h2_ratio = 1.0  # actual ratio
        self.optimal_n2_h2_ratio = 1.0/3.0  # stoichiometric ratio (N2:H2 = 1:3)
        
        # Product quality
        self.nh3_purity = 99.5  # %
        self.water_content = 500.0  # ppm
        self.unreacted_gases = 5.0  # %
        
        # Environmental metrics
        self.co2_emissions = 0.0  # kg/h
        self.water_usage = 0.0  # L/h
        
        # Maintenance indicators
        self.maintenance_due = MaintenanceType.NONE
        self.next_maintenance_hours = 8760.0  # 1 year
        self.pressure_vessel_inspection_due = False
        
        # Random number generator
        self.rng = np.random.RandomState()
        
        # System flows
        self.cooling_water_flow = 0.0  # m³/h
    
    def update_catalyst_deactivation(self, temperature: float, pressure: float, dt_hours: float):
        """
        Update catalyst deactivation based on operating conditions
        """
        # Base deactivation rate (per hour)
        base_deactivation = 1.0 / self.config.hb_catalyst_life
        
        # Temperature effect (higher temp = faster deactivation)
        temp_factor = math.exp((temperature - 450) / 50)  # Exponential temperature dependence
        
        # Pressure effect (higher pressure = slower deactivation)
        pressure_factor = 200.0 / max(pressure, 50.0)
        
        # Operating hours effect (catalyst aging)
        aging_factor = 1.0 + (self.catalyst_age / self.config.hb_catalyst_life) * 0.5
        
        # Apply deactivation
        deactivation_rate = base_deactivation * temp_factor * pressure_factor * aging_factor
        self.catalyst_activity = max(0.1, self.catalyst_activity - deactivation_rate * dt_hours)
        self.catalyst_age += dt_hours
        
        # Check if catalyst replacement is needed
        if self.catalyst_activity < 0.6:
            self.catalyst_replacement_due = True
    
    def calculate_reaction_kinetics(self, n2_rate: float, h2_rate: float, 
                                  temperature: float, pressure: float) -> Dict[str, float]:
        """
        Calculate reaction kinetics for N2 + 3H2 -> 2NH3
        """
        if n2_rate <= 0 or h2_rate <= 0:
            return {"nh3_rate": 0.0, "conversion": 0.0, "selectivity": 0.98}
        
        # Check stoichiometry (N2:H2 = 1:3 molar ratio)
        # Molecular weights: N2 = 28, H2 = 2, NH3 = 17
        n2_moles = n2_rate / 28.0  # kmol/h
        h2_moles = h2_rate / 2.0   # kmol/h
        
        # Determine limiting reactant
        stoichiometric_h2 = n2_moles * 3.0
        if h2_moles < stoichiometric_h2:
            # H2 is limiting
            limiting_h2 = h2_moles
            limiting_n2 = h2_moles / 3.0
        else:
            # N2 is limiting
            limiting_n2 = n2_moles
            limiting_h2 = n2_moles * 3.0
        
        # Calculate equilibrium constant (temperature dependent)
        # K = exp(A - B/T) where T is in Kelvin
        T_kelvin = temperature + 273.15
        log_K = 10.0 - 4000.0 / T_kelvin  # Simplified equilibrium constant
        K_eq = math.exp(log_K)
        
        # Calculate conversion based on equilibrium and kinetics
        # Simplified conversion model
        base_conversion = K_eq / (1 + K_eq)
        
        # Pressure effect on conversion (Le Chatelier's principle)
        pressure_factor = math.log(pressure / 1.0) / math.log(200.0)
        
        # Catalyst activity effect
        catalyst_factor = self.catalyst_activity ** 0.5
        
        # Temperature effect on kinetics
        temp_factor = math.exp(-(temperature - 450) ** 2 / 1000)  # Optimal around 450°C
        
        # Overall conversion
        conversion = base_conversion * pressure_factor * catalyst_factor * temp_factor
        conversion = max(0.1, min(0.9, conversion))  # Practical limits
        
        # Calculate NH3 production rate
        nh3_moles = limiting_n2 * 2.0 * conversion  # 2 moles NH3 per mole N2
        nh3_rate = nh3_moles * 17.0  # kg/h
        
        # Selectivity (decreases with temperature)
        selectivity = 0.98 - (temperature - 450) / 1000
        selectivity = max(0.9, min(0.99, selectivity))
        
        return {
            "nh3_rate": nh3_rate,
            "conversion": conversion,
            "selectivity": selectivity,
            "limiting_n2": limiting_n2,
            "limiting_h2": limiting_h2
        }
    
    def calculate_energy_consumption(self, nh3_rate: float, conversion: float) -> float:
        """
        Calculate energy consumption for the reactor
        """
        if nh3_rate <= 0:
            return 0.0
        
        # Theoretical energy requirement (GJ/t NH3)
        theoretical_energy = 28.0  # GJ/t NH3
        
        # Actual energy based on efficiency
        actual_energy = theoretical_energy / (conversion * self.heat_recovery_efficiency * 
                                            self.compression_efficiency)
        
        # Convert to MW
        energy_mw = (nh3_rate * actual_energy) / 3.6  # MW
        
        return energy_mw
    
    def calculate_product_quality(self, nh3_rate: float, selectivity: float) -> Dict[str, float]:
        """
        Calculate product quality metrics
        """
        if nh3_rate <= 0:
            return {"purity": 99.5, "water_content": 500.0, "unreacted_gases": 0.0}
        
        # NH3 purity (affected by selectivity and separation efficiency)
        purity = selectivity * self.separation_efficiency * 100
        purity += self.rng.normal(0, 0.1)  # Add noise
        purity = max(98.0, min(99.9, purity))
        
        # Water content (increases with poor separation)
        water_content = 500.0 + (1.0 - self.separation_efficiency) * 2000
        water_content += self.rng.normal(0, 50)
        water_content = max(100, min(2000, water_content))
        
        # Unreacted gases (decreases with conversion)
        unreacted_gases = (1.0 - selectivity) * 10
        unreacted_gases += self.rng.normal(0, 0.5)
        unreacted_gases = max(0.5, min(10.0, unreacted_gases))
        
        return {
            "purity": purity,
            "water_content": water_content,
            "unreacted_gases": unreacted_gases
        }
    
    def calculate_environmental_impact(self, nh3_rate: float, energy_consumption: float) -> Dict[str, float]:
        """
        Calculate environmental impact metrics
        """
        if nh3_rate <= 0:
            return {"co2_emissions": 0.0, "water_usage": 0.0}
        
        # CO2 emissions (depends on energy source)
        # Assuming mixed energy source: 0.5 kg CO2/kWh
        co2_emissions = energy_consumption * 1000 * 0.5  # kg/h
        
        # Water usage (cooling and steam generation)
        water_usage = nh3_rate * 50  # L/h per t/h NH3
        
        return {
            "co2_emissions": co2_emissions,
            "water_usage": water_usage
        }
    
    def check_maintenance_requirements(self):
        """
        Check if maintenance is required
        """
        # Check catalyst replacement
        if self.catalyst_replacement_due:
            self.maintenance_due = MaintenanceType.MAJOR
            return
        
        # Check catalyst activity
        if self.catalyst_activity < 0.7:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Check conversion efficiency
        if self.conversion_efficiency < 0.7:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        # Pressure vessel inspection (every 2 years)
        if self.operating_hours >= 17520:  # 2 years
            self.pressure_vessel_inspection_due = True
            self.maintenance_due = MaintenanceType.MAJOR
            return
        
        # Regular maintenance every 6 months
        if self.hours_since_maintenance >= 4380:
            self.maintenance_due = MaintenanceType.LIGHT
            return
        
        self.maintenance_due = MaintenanceType.NONE
    
    def perform_maintenance(self, maintenance_type: MaintenanceType):
        """
        Perform maintenance and update equipment state
        """
        if maintenance_type == MaintenanceType.LIGHT:
            # Light maintenance - optimization and minor repairs
            self.hours_since_maintenance = 0.0
            self.conversion_efficiency = min(self.config.hb_conversion_efficiency, 
                                           self.conversion_efficiency + 0.05)
            self.heat_recovery_efficiency = min(0.85, self.heat_recovery_efficiency + 0.02)
            
        elif maintenance_type == MaintenanceType.MAJOR:
            # Major maintenance - catalyst replacement, pressure vessel inspection
            self.hours_since_maintenance = 0.0
            self.next_maintenance_hours = self.operating_hours + 8760  # 1 year
            
            if self.catalyst_replacement_due:
                self.catalyst_activity = 1.0
                self.catalyst_age = 0.0
                self.catalyst_replacement_due = False
            
            if self.pressure_vessel_inspection_due:
                self.pressure_vessel_inspection_due = False
            
            # Restore efficiencies
            self.conversion_efficiency = self.config.hb_conversion_efficiency
            self.heat_recovery_efficiency = 0.8
            self.compression_efficiency = 0.85
            self.separation_efficiency = 0.95
        
        self.maintenance_due = MaintenanceType.NONE
    
    def set_production_demand(self, nh3_demand: float, n2_available: float, h2_available: float):
        """
        Set production demand and calculate required reactant flows
        """
        if nh3_demand > 0 and self.status == EquipmentStatus.IDLE:
            self.status = EquipmentStatus.RUNNING
        elif nh3_demand <= 0 and self.status == EquipmentStatus.RUNNING:
            self.status = EquipmentStatus.IDLE
        
        if nh3_demand <= 0:
            self.nh3_production_rate = 0.0
            self.n2_feed_rate = 0.0
            self.h2_feed_rate = 0.0
            self.energy_consumption = 0.0
            return
        
        # Calculate required reactant flows for NH3 production
        # Stoichiometry: N2 + 3H2 -> 2NH3
        # Molecular weights: N2 = 28, H2 = 2, NH3 = 17
        
        # Calculate theoretical reactant requirements
        nh3_moles_needed = (nh3_demand * 1000) / 17.0  # kmol/h
        n2_moles_needed = nh3_moles_needed / 2.0  # kmol/h
        h2_moles_needed = nh3_moles_needed * 3.0 / 2.0  # kmol/h
        
        # Convert to mass flow rates
        n2_needed = n2_moles_needed * 28.0  # kg/h
        h2_needed = h2_moles_needed * 2.0  # kg/h
        
        # Account for conversion efficiency
        n2_feed_required = n2_needed / self.conversion_efficiency
        h2_feed_required = h2_needed / self.conversion_efficiency
        
        # Limit by available reactants
        self.n2_feed_rate = min(n2_feed_required, n2_available)
        self.h2_feed_rate = min(h2_feed_required, h2_available)
        
        # Calculate actual N2:H2 ratio
        n2_moles_actual = self.n2_feed_rate / 28.0
        h2_moles_actual = self.h2_feed_rate / 2.0
        self.n2_h2_ratio = n2_moles_actual / h2_moles_actual if h2_moles_actual > 0 else 0
        
        # Calculate reaction kinetics
        reaction_data = self.calculate_reaction_kinetics(
            self.n2_feed_rate, self.h2_feed_rate, 
            self.reactor_temperature, self.reactor_pressure
        )
        
        # Update production rate and efficiency
        self.nh3_production_rate = reaction_data["nh3_rate"] / 1000  # t/h
        self.conversion_efficiency = reaction_data["conversion"]
        self.selectivity = reaction_data["selectivity"]
        
        # Calculate energy consumption
        self.energy_consumption = self.calculate_energy_consumption(
            self.nh3_production_rate, self.conversion_efficiency
        )
        
        # Update product quality
        quality_data = self.calculate_product_quality(self.nh3_production_rate, self.selectivity)
        self.nh3_purity = quality_data["purity"]
        self.water_content = quality_data["water_content"]
        self.unreacted_gases = quality_data["unreacted_gases"]
        
        # Calculate environmental impact
        env_data = self.calculate_environmental_impact(self.nh3_production_rate, self.energy_consumption)
        self.co2_emissions = env_data["co2_emissions"]
        self.water_usage = env_data["water_usage"]
        
        # Update cooling water flow
        self.cooling_water_flow = self.energy_consumption * 100  # m³/h (rough estimate)
    
    def update_state(self, dt_hours: float):
        """
        Update equipment state for time step
        """
        if self.status == EquipmentStatus.RUNNING:
            # Update operating hours
            self.operating_hours += dt_hours
            self.hours_since_maintenance += dt_hours
            
            # Update cumulative production
            self.nh3_production_cumulative += self.nh3_production_rate * dt_hours
            
            # Update catalyst deactivation
            self.update_catalyst_deactivation(
                self.reactor_temperature, self.reactor_pressure, dt_hours
            )
            
            # Update process temperatures (with some variation)
            self.catalyst_bed_temperature = self.reactor_temperature + self.rng.normal(0, 5)
        
        # Check maintenance requirements
        self.check_maintenance_requirements()
    
    def generate_telemetry_data(self, timestamp: Optional[datetime] = None) -> HaberBoschData:
        """
        Generate complete telemetry data point
        """
        if timestamp is None:
            timestamp = self.current_time
        
        return HaberBoschData(
            timestamp=timestamp,
            equipment_id=self.equipment_id,
            location_id=self.location_id,
            
            status=self.status,
            operating_hours=self.operating_hours,
            
            nh3_production_rate=self.nh3_production_rate,
            nh3_production_cumulative=self.nh3_production_cumulative,
            conversion_efficiency=self.conversion_efficiency,
            energy_consumption=self.energy_consumption,
            
            n2_feed_rate=self.n2_feed_rate,
            h2_feed_rate=self.h2_feed_rate,
            n2_h2_ratio=self.n2_h2_ratio,
            optimal_n2_h2_ratio=self.optimal_n2_h2_ratio,
            
            reactor_temperature=self.reactor_temperature,
            reactor_pressure=self.reactor_pressure,
            catalyst_bed_temperature=self.catalyst_bed_temperature,
            cooling_water_flow=self.cooling_water_flow,
            
            catalyst_activity=self.catalyst_activity,
            catalyst_age=self.catalyst_age,
            catalyst_replacement_due=self.catalyst_replacement_due,
            selectivity=self.selectivity,
            
            nh3_purity=self.nh3_purity,
            water_content=self.water_content,
            unreacted_gases=self.unreacted_gases,
            
            heat_recovery_efficiency=self.heat_recovery_efficiency,
            compression_efficiency=self.compression_efficiency,
            separation_efficiency=self.separation_efficiency,
            
            maintenance_due=self.maintenance_due,
            hours_since_maintenance=self.hours_since_maintenance,
            pressure_vessel_inspection_due=self.pressure_vessel_inspection_due,
            
            co2_emissions=self.co2_emissions,
            water_usage=self.water_usage
        )
    
    def step(self, nh3_demand: float, n2_available: float, h2_available: float, 
             dt_hours: float = 1/3600):
        """
        Single simulation step
        """
        # Set production demand
        self.set_production_demand(nh3_demand, n2_available, h2_available)
        
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
        self.catalyst_activity = 1.0
        self.catalyst_age = 0.0
        self.catalyst_replacement_due = False
        self.conversion_efficiency = self.config.hb_conversion_efficiency
        self.nh3_production_rate = 0.0
        self.nh3_production_cumulative = 0.0
        self.energy_consumption = 0.0
        self.maintenance_due = MaintenanceType.NONE
        self.pressure_vessel_inspection_due = False
        self.current_time = datetime.now()