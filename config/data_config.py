# config/data_config.py

from dataclasses import dataclass
from typing import Dict, List, Optional
import math

@dataclass
class WeatherConfig:
    """Configuration for weather data generation"""
    
    # Location parameters (Casablanca, Morocco as example)
    latitude: float = 33.5731  # degrees
    longitude: float = -7.5898  # degrees
    timezone: str = "Africa/Casablanca"
    
    # Solar irradiance parameters
    solar_max_irradiance: float = 1000.0  # W/m² (clear sky peak)
    solar_min_irradiance: float = 50.0    # W/m² (heavy clouds)
    solar_noise_std: float = 0.1          # Standard deviation for noise
    cloud_probability: float = 0.3        # Probability of cloud events
    cloud_duration_mean: float = 30.0     # Mean cloud duration (minutes)
    cloud_intensity_mean: float = 0.6     # Mean cloud intensity (0-1)
    
    # Wind parameters
    wind_mean_speed: float = 7.5          # m/s (average wind speed)
    wind_weibull_shape: float = 2.0       # Weibull shape parameter
    wind_weibull_scale: float = 8.5       # Weibull scale parameter
    wind_turbulence_std: float = 0.8      # Standard deviation for turbulence
    wind_direction_std: float = 15.0      # Standard deviation for direction (degrees)
    wind_gust_probability: float = 0.15   # Probability of gust events
    wind_gust_multiplier: float = 1.5     # Gust intensity multiplier
    
    # Temperature parameters
    temp_annual_mean: float = 20.0        # °C (annual average)
    temp_annual_amplitude: float = 8.0    # °C (seasonal variation)
    temp_diurnal_amplitude: float = 12.0  # °C (daily variation)
    temp_noise_std: float = 1.0           # Standard deviation for noise
    
    # Temporal parameters
    sampling_interval: float = 1.0        # seconds between samples
    batch_size: int = 100                 # messages per batch
    
    # Data quality parameters
    missing_data_probability: float = 0.001  # Probability of missing data
    outlier_probability: float = 0.005       # Probability of outliers

@dataclass
class EquipmentConfig:
    """Configuration for equipment simulation"""
    
    # Electrolyzer parameters
    electrolyzer_rated_power: float = 10.0      # MW
    electrolyzer_efficiency_nominal: float = 0.7  # efficiency at rated power
    electrolyzer_min_load: float = 0.2          # minimum load factor
    electrolyzer_degradation_rate: float = 0.001  # per hour
    
    # ASU parameters
    asu_rated_capacity: float = 500.0           # Nm³/h N₂ production
    asu_power_consumption: float = 0.5          # kWh/Nm³
    asu_purity_nominal: float = 99.9            # % N₂ purity
    
    # Haber-Bosch parameters
    hb_rated_capacity: float = 100.0            # t/day NH₃ production
    hb_conversion_efficiency: float = 0.85      # N₂+H₂ → NH₃ conversion
    hb_catalyst_life: float = 8760.0            # hours
    
    # Storage parameters
    h2_storage_capacity: float = 1000.0         # kg H₂
    battery_capacity: float = 50.0              # MWh
    battery_efficiency: float = 0.95            # round-trip efficiency

@dataclass
class KafkaConfig:
    """Kafka configuration"""
    
    bootstrap_servers: List[str] = None
    topic_config: Dict[str, Dict] = None
    
    def __post_init__(self):
        if self.bootstrap_servers is None:
            self.bootstrap_servers = ["localhost:9092"]
            
        if self.topic_config is None:
            self.topic_config = {
                "weather-raw": {
                    "partitions": 3,
                    "replication_factor": 1,
                    "retention_ms": 7 * 24 * 60 * 60 * 1000,  # 7 days
                    "cleanup_policy": "delete"
                },
                "equipment-telemetry": {
                    "partitions": 6,
                    "replication_factor": 1,
                    "retention_ms": 30 * 24 * 60 * 60 * 1000,  # 30 days
                    "cleanup_policy": "delete"
                },
                "energy-consumption": {
                    "partitions": 3,
                    "replication_factor": 1,
                    "retention_ms": 7 * 24 * 60 * 60 * 1000,  # 7 days
                    "cleanup_policy": "delete"
                }
            }

# Global configuration instances
WEATHER_CONFIG = WeatherConfig()
EQUIPMENT_CONFIG = EquipmentConfig()
KAFKA_CONFIG = KafkaConfig()