# data_producers/equipment_orchestrator.py

import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import json

from data_producers.electrolyzer_simulator import ElectrolyzerSimulator
from data_producers.asu_simulator import ASUSimulator
from data_producers.haber_bosch_simulator import HaberBoschSimulator
from data_producers.energy_storage_simulators import BatteryStorageSimulator, H2StorageSimulator
from data_producers.weather_generator import WeatherGenerator
from schemas.equipment_schema import SystemOverviewData, EquipmentStatus, MaintenanceType
from config.data_config import EQUIPMENT_CONFIG, WEATHER_CONFIG

class EquipmentOrchestrator:
    """
    Orchestrates all equipment simulators to create a realistic
    power-to-ammonia production system simulation
    """
    
    def __init__(self, plant_id: str = "plant_001", location_id: str = "casablanca_001"):
        self.plant_id = plant_id
        self.location_id = location_id
        self.current_time = datetime.now()
        
        # Initialize all equipment simulators
        self.electrolyzer = ElectrolyzerSimulator(
            equipment_id=f"{plant_id}_electrolyzer_001",
            location_id=location_id
        )
        
        self.asu = ASUSimulator(
            equipment_id=f"{plant_id}_asu_001",
            location_id=location_id
        )
        
        self.haber_bosch = HaberBoschSimulator(
            equipment_id=f"{plant_id}_haber_bosch_001",
            location_id=location_id
        )
        
        self.battery_storage = BatteryStorageSimulator(
            equipment_id=f"{plant_id}_battery_001",
            location_id=location_id
        )
        
        self.h2_storage = H2StorageSimulator(
            equipment_id=f"{plant_id}_h2_storage_001",
            location_id=location_id
        )
        
        self.weather_generator = WeatherGenerator(
            location_id=location_id
        )
        
        # System state tracking
        self.system_power_demand = 0.0  # MW
        self.renewable_power_available = 0.0  # MW
        self.grid_power_usage = 0.0  # MW
        self.nh3_production_target = 0.0  # t/h
        
        # Production flows
        self.h2_production_rate = 0.0  # kg/h
        self.h2_consumption_rate = 0.0  # kg/h
        self.n2_production_rate = 0.0  # Nm³/h
        self.n2_consumption_rate = 0.0  # Nm³/h
        
        # System metrics
        self.overall_efficiency = 0.0
        self.capacity_factor = 0.0
        self.system_availability = 1.0
        self.planned_downtime = 0.0
        self.unplanned_downtime = 0.0
        
        # Economic metrics
        self.operational_cost = 0.0  # €/h
        self.maintenance_cost = 0.0  # €/h
        self.energy_cost = 0.0  # €/MWh
        
        # Random number generator
        self.rng = np.random.RandomState()
        
        # Equipment list for easy iteration
        self.equipment_list = [
            self.electrolyzer,
            self.asu,
            self.haber_bosch,
            self.battery_storage,
            self.h2_storage
        ]
    
    def calculate_renewable_power_available(self, weather_data) -> float:
        """
        Calculate available renewable power from weather data
        """
        solar_power = weather_data.solar_power_potential  # kW
        wind_power = weather_data.wind_power_potential    # kW
        
        # Convert to MW and add some variability
        total_renewable_mw = (solar_power + wind_power) / 1000.0
        
        # Add grid availability factor
        grid_factor = 0.95 + self.rng.normal(0, 0.02)  # 95% ± 2% availability
        
        return total_renewable_mw * grid_factor
    
    def balance_power_flows(self, renewable_power: float, nh3_target: float) -> Dict[str, float]:
        """
        Balance power flows across the system to meet NH3 production target
        """
        # Calculate power demand from each equipment
        electrolyzer_demand = min(renewable_power * 0.7, EQUIPMENT_CONFIG.electrolyzer_rated_power)
        asu_demand = self.asu.compressor_power / 1000.0 + self.asu.refrigeration_power / 1000.0  # MW
        haber_bosch_demand = self.haber_bosch.energy_consumption  # MW
        
        # Total system demand
        total_demand = electrolyzer_demand + asu_demand + haber_bosch_demand
        
        # Power balancing logic
        if total_demand <= renewable_power:
            # Surplus renewable power - charge battery
            surplus_power = renewable_power - total_demand
            battery_charge_power = min(surplus_power, EQUIPMENT_CONFIG.battery_capacity * 0.5)
            grid_power = 0.0
        else:
            # Deficit - discharge battery and/or use grid
            deficit = total_demand - renewable_power
            battery_discharge_power = min(deficit, 
                                        self.battery_storage.capacity_available * 
                                        self.battery_storage.state_of_charge * 0.5)
            grid_power = max(0, deficit - battery_discharge_power)
            battery_charge_power = -battery_discharge_power
        
        return {
            "electrolyzer_power": electrolyzer_demand,
            "asu_power": asu_demand,
            "haber_bosch_power": haber_bosch_demand,
            "battery_power": battery_charge_power,
            "grid_power": grid_power
        }
    
    def balance_material_flows(self) -> Dict[str, float]:
        """
        Balance material flows (H2, N2, NH3) across the system
        """
        # H2 production from electrolyzer
        h2_production = self.electrolyzer.h2_production_rate  # kg/h
        
        # N2 production from ASU
        n2_production = self.asu.n2_production_rate  # Nm³/h
        
        # Current H2 and N2 storage levels
        h2_stored = self.h2_storage.h2_mass_stored  # kg
        n2_available = n2_production + 100  # Assume some buffer storage
        
        # Calculate material flows to Haber-Bosch
        # Stoichiometry: N2 + 3H2 -> 2NH3
        # Need to balance based on available materials
        
        # Available H2 (from production + storage)
        available_h2 = h2_production + min(h2_stored, 100)  # kg/h from storage
        
        # Available N2 (convert Nm³/h to kg/h: 1 Nm³ N2 ≈ 1.25 kg)
        available_n2_kg = n2_available * 1.25  # kg/h
        
        # Calculate optimal feed rates based on stoichiometry
        # N2:H2 mass ratio = 28:6 = 4.67:1
        optimal_h2_for_n2 = available_n2_kg / 4.67
        optimal_n2_for_h2 = available_h2 * 4.67
        
        # Determine limiting reactant
        if optimal_h2_for_n2 <= available_h2:
            # N2 is limiting
            h2_to_hb = optimal_h2_for_n2
            n2_to_hb = available_n2_kg
        else:
            # H2 is limiting
            h2_to_hb = available_h2
            n2_to_hb = optimal_n2_for_h2
        
        # H2 storage flows
        h2_to_storage = max(0, h2_production - h2_to_hb)
        h2_from_storage = max(0, h2_to_hb - h2_production)
        
        return {
            "h2_production": h2_production,
            "h2_to_hb": h2_to_hb,
            "h2_to_storage": h2_to_storage,
            "h2_from_storage": h2_from_storage,
            "n2_production": n2_production,
            "n2_to_hb": n2_to_hb / 1.25,  # Convert back to Nm³/h
            "n2_to_hb_kg": n2_to_hb
        }
    
    def calculate_system_metrics(self, power_flows: Dict, material_flows: Dict) -> Dict[str, float]:
        """
        Calculate overall system performance metrics
        """
        # Total power consumption
        total_power = (power_flows["electrolyzer_power"] + 
                      power_flows["asu_power"] + 
                      power_flows["haber_bosch_power"])
        
        # Overall efficiency (renewable energy to NH3)
        nh3_energy_content = self.haber_bosch.nh3_production_rate * 18.6  # MJ/kg NH3
        renewable_energy_input = self.renewable_power_available * 3.6  # MJ/h
        
        if renewable_energy_input > 0:
            overall_efficiency = (nh3_energy_content / 1000) / renewable_energy_input  # MW/MW
        else:
            overall_efficiency = 0.0
        
        # Capacity factor
        max_nh3_production = EQUIPMENT_CONFIG.hb_rated_capacity / 24  # t/h
        capacity_factor = self.haber_bosch.nh3_production_rate / max_nh3_production if max_nh3_production > 0 else 0.0
        
        # System availability (based on equipment status)
        equipment_availability = []
        for equipment in self.equipment_list:
            if equipment.status in [EquipmentStatus.RUNNING, EquipmentStatus.IDLE]:
                equipment_availability.append(1.0)
            elif equipment.status == EquipmentStatus.MAINTENANCE:
                equipment_availability.append(0.5)
            else:
                equipment_availability.append(0.0)
        
        system_availability = min(equipment_availability) if equipment_availability else 0.0
        
        # Power utilization efficiency
        power_utilization = total_power / max(self.renewable_power_available, 0.1)
        
        return {
            "overall_efficiency": overall_efficiency,
            "capacity_factor": capacity_factor,
            "system_availability": system_availability,
            "power_utilization": power_utilization
        }
    
    def calculate_economic_metrics(self, power_flows: Dict) -> Dict[str, float]:
        """
        Calculate economic metrics
        """
        # Energy costs (€/MWh)
        renewable_cost = 50.0  # €/MWh
        grid_cost = 80.0  # €/MWh
        
        # Calculate energy cost
        renewable_energy_cost = self.renewable_power_available * renewable_cost
        grid_energy_cost = power_flows["grid_power"] * grid_cost
        total_energy_cost = renewable_energy_cost + grid_energy_cost
        
        # Operational costs (simplified)
        operational_cost = total_energy_cost * 0.1  # 10% of energy cost
        
        # Maintenance costs (based on equipment condition)
        maintenance_cost = 0.0
        for equipment in self.equipment_list:
            if hasattr(equipment, 'maintenance_due') and equipment.maintenance_due != MaintenanceType.NONE:
                if equipment.maintenance_due == MaintenanceType.LIGHT:
                    maintenance_cost += 100.0  # €/h
                elif equipment.maintenance_due == MaintenanceType.MAJOR:
                    maintenance_cost += 500.0  # €/h
                elif equipment.maintenance_due == MaintenanceType.EMERGENCY:
                    maintenance_cost += 1000.0  # €/h
        
        return {
            "operational_cost": operational_cost,
            "maintenance_cost": maintenance_cost,
            "energy_cost": total_energy_cost
        }
    
    def step(self, nh3_production_target: float = 4.0, dt_hours: float = 1/3600):
        """
        Execute one simulation step for the entire system
        """
        # Generate weather data
        weather_data = self.weather_generator.generate_weather_data(self.current_time)
        
        # Calculate available renewable power
        self.renewable_power_available = self.calculate_renewable_power_available(weather_data)
        
        # Balance power flows
        power_flows = self.balance_power_flows(self.renewable_power_available, nh3_production_target)
        
        # Update equipment with power demands
        # Electrolyzer
        electrolyzer_data = self.electrolyzer.step(
            power_flows["electrolyzer_power"], 
            weather_data.temperature, 
            dt_hours
        )
        
        # ASU (needs N2 demand from Haber-Bosch)
        n2_demand = nh3_production_target * 0.82 * 800  # Rough estimate: t/h NH3 -> Nm³/h N2
        asu_data = self.asu.step(n2_demand, 0.0, dt_hours)
        
        # Balance material flows
        material_flows = self.balance_material_flows()
        
        # H2 Storage
        h2_storage_data = self.h2_storage.step(
            material_flows["h2_to_storage"],
            material_flows["h2_from_storage"],
            weather_data.temperature,
            dt_hours
        )
        
        # Haber-Bosch reactor
        haber_bosch_data = self.haber_bosch.step(
            nh3_production_target,
            material_flows["n2_to_hb_kg"],
            material_flows["h2_to_hb"],
            dt_hours
        )
        
        # Battery storage
        battery_data = self.battery_storage.step(
            power_flows["battery_power"],
            weather_data.temperature,
            dt_hours
        )
        
        # Calculate system metrics
        system_metrics = self.calculate_system_metrics(power_flows, material_flows)
        economic_metrics = self.calculate_economic_metrics(power_flows)
        
        # Update system state
        self.system_power_demand = (power_flows["electrolyzer_power"] + 
                                   power_flows["asu_power"] + 
                                   power_flows["haber_bosch_power"])
        self.grid_power_usage = power_flows["grid_power"]
        self.overall_efficiency = system_metrics["overall_efficiency"]
        self.capacity_factor = system_metrics["capacity_factor"]
        self.system_availability = system_metrics["system_availability"]
        self.operational_cost = economic_metrics["operational_cost"]
        self.maintenance_cost = economic_metrics["maintenance_cost"]
        self.energy_cost = economic_metrics["energy_cost"]
        
        # Update flows
        self.h2_production_rate = material_flows["h2_production"]
        self.h2_consumption_rate = material_flows["h2_to_hb"]
        self.n2_production_rate = material_flows["n2_production"]
        self.n2_consumption_rate = material_flows["n2_to_hb"]
        
        # Update time
        self.current_time += timedelta(hours=dt_hours)
        
        # Return all data
        return {
            "timestamp": self.current_time,
            "weather": weather_data,
            "electrolyzer": electrolyzer_data,
            "asu": asu_data,
            "haber_bosch": haber_bosch_data,
            "battery": battery_data,
            "h2_storage": h2_storage_data,
            "system_overview": self.generate_system_overview(),
            "power_flows": power_flows,
            "material_flows": material_flows
        }
    
    def generate_system_overview(self) -> SystemOverviewData:
        """
        Generate system overview data
        """
        return SystemOverviewData(
            timestamp=self.current_time,
            system_id=self.plant_id,
            location_id=self.location_id,
            
            total_power_consumption=self.system_power_demand,
            renewable_power_available=self.renewable_power_available,
            grid_power_usage=self.grid_power_usage,
            power_utilization_efficiency=min(1.0, self.system_power_demand / max(self.renewable_power_available, 0.1)),
            
            nh3_production_rate=self.haber_bosch.nh3_production_rate,
            h2_production_rate=self.h2_production_rate,
            n2_production_rate=self.n2_production_rate,
            
            overall_efficiency=self.overall_efficiency,
            capacity_factor=self.capacity_factor,
            
            system_availability=self.system_availability,
            planned_downtime=self.planned_downtime,
            unplanned_downtime=self.unplanned_downtime,
            
            operational_cost=self.operational_cost,
            maintenance_cost=self.maintenance_cost,
            energy_cost=self.energy_cost
        )
    
    def run_simulation(self, duration_hours: float = 24.0, dt_hours: float = 1/60, 
                      nh3_target: float = 4.0) -> List[Dict]:
        """
        Run simulation for specified duration
        """
        results = []
        steps = int(duration_hours / dt_hours)
        
        for i in range(steps):
            # Vary NH3 target slightly to simulate demand variation
            target_variation = self.rng.normal(0, 0.1)
            current_target = max(0.1, nh3_target + target_variation)
            
            # Execute step
            step_result = self.step(current_target, dt_hours)
            results.append(step_result)
            
            # Progress indicator
            if i % (steps // 10) == 0:
                progress = (i / steps) * 100
                print(f"Simulation progress: {progress:.1f}%")
        
        return results
    
    def get_equipment_status(self) -> Dict[str, Any]:
        """
        Get current status of all equipment
        """
        return {
            "electrolyzer": {
                "status": self.electrolyzer.status.value,
                "efficiency": self.electrolyzer.efficiency_current,
                "power": self.electrolyzer.current_power,
                "h2_production": self.electrolyzer.h2_production_rate,
                "maintenance_due": self.electrolyzer.maintenance_due.value
            },
            "asu": {
                "status": self.asu.status.value,
                "n2_production": self.asu.n2_production_rate,
                "efficiency": self.asu.production_efficiency,
                "maintenance_due": self.asu.maintenance_due.value
            },
            "haber_bosch": {
                "status": self.haber_bosch.status.value,
                "nh3_production": self.haber_bosch.nh3_production_rate,
                "conversion_efficiency": self.haber_bosch.conversion_efficiency,
                "maintenance_due": self.haber_bosch.maintenance_due.value
            },
            "battery": {
                "status": self.battery_storage.status.value,
                "soc": self.battery_storage.state_of_charge,
                "health": self.battery_storage.state_of_health,
                "maintenance_due": self.battery_storage.maintenance_due.value
            },
            "h2_storage": {
                "status": self.h2_storage.status.value,
                "fill_level": self.h2_storage.fill_level,
                "pressure": self.h2_storage.pressure,
                "maintenance_due": self.h2_storage.maintenance_due.value
            },
            "system": {
                "efficiency": self.overall_efficiency,
                "availability": self.system_availability,
                "capacity_factor": self.capacity_factor
            }
        }
    
    def reset_all_equipment(self):
        """
        Reset all equipment to initial state
        """
        for equipment in self.equipment_list:
            equipment.reset()
        
        # Reset system state
        self.system_power_demand = 0.0
        self.renewable_power_available = 0.0
        self.grid_power_usage = 0.0
        self.current_time = datetime.now()
        
        print("🔄 All equipment reset to initial state")