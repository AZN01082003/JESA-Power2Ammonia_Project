# data_producers/weather_generator.py

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math
import random
from dataclasses import dataclass

from config.data_config import WEATHER_CONFIG
from schemas.weather_schema import WeatherData, WeatherAlert

class WeatherGenerator:
    """
    Advanced weather data generator with realistic stochastic patterns
    for solar irradiance, wind, and temperature data.
    """
    
    def __init__(self, config=None, location_id: str = "casablanca_001"):
        self.config = config or WEATHER_CONFIG
        self.location_id = location_id
        self.current_time = datetime.now()
        
        # Initialize state variables for continuity
        self.wind_direction_state = random.uniform(0, 360)
        self.cloud_state = 0.0
        self.cloud_duration_remaining = 0
        self.temperature_state = self.config.temp_annual_mean
        self.wind_speed_state = self.config.wind_mean_speed
        
        # Seasonal and daily patterns
        self.seasonal_phase = 0.0
        self.daily_phase = 0.0
        
        # Initialize random seeds for reproducibility if needed
        self.rng = np.random.RandomState()
        
    def get_solar_position(self, timestamp: datetime) -> Tuple[float, float]:
        """
        Calculate solar elevation and azimuth angles
        Returns: (elevation_angle, azimuth_angle) in degrees
        """
        # Convert to day of year and hour
        day_of_year = timestamp.timetuple().tm_yday
        hour = timestamp.hour + timestamp.minute / 60.0 + timestamp.second / 3600.0
        
        # Solar declination
        declination = 23.45 * math.sin(math.radians(360 * (284 + day_of_year) / 365))
        
        # Hour angle
        hour_angle = 15.0 * (hour - 12.0)
        
        # Solar elevation
        lat_rad = math.radians(self.config.latitude)
        dec_rad = math.radians(declination)
        hour_rad = math.radians(hour_angle)
        
        elevation = math.degrees(math.asin(
            math.sin(lat_rad) * math.sin(dec_rad) +
            math.cos(lat_rad) * math.cos(dec_rad) * math.cos(hour_rad)
        ))
        
        # Solar azimuth
        azimuth = math.degrees(math.atan2(
            math.sin(hour_rad),
            math.cos(hour_rad) * math.sin(lat_rad) - math.tan(dec_rad) * math.cos(lat_rad)
        ))
        
        # Normalize azimuth to 0-360
        azimuth = (azimuth + 360) % 360
        
        return max(0, elevation), azimuth
    
    def generate_solar_irradiance(self, timestamp: datetime) -> Dict[str, float]:
        """
        Generate realistic solar irradiance with cloud effects
        """
        elevation, azimuth = self.get_solar_position(timestamp)
        
        # Update cloud state (always, regardless of day/night)
        cloud_cover = self._update_cloud_state()
        
        # Base irradiance from solar angle
        if elevation <= 0:
            # Night time
            base_irradiance = 0.0
            direct_irradiance = 0.0
            diffuse_irradiance = 0.0
        else:
            # Day time - calculate clear sky irradiance
            air_mass = 1.0 / (math.sin(math.radians(elevation)) + 0.50572 * (elevation + 6.07995)**(-1.6364))
            clear_sky_irradiance = self.config.solar_max_irradiance * math.sin(math.radians(elevation)) * (0.7**(air_mass**0.678))
            
            # Cloud effects
            cloud_reduction = 1.0 - (cloud_cover * 0.8)  # Clouds reduce irradiance by up to 80%
            
            base_irradiance = clear_sky_irradiance * cloud_reduction
            
            # Split into direct and diffuse components
            direct_fraction = 1.0 - cloud_cover * 0.9
            direct_irradiance = base_irradiance * direct_fraction
            diffuse_irradiance = base_irradiance * (1.0 - direct_fraction)
        
        # Add realistic noise (only during daytime)
        if base_irradiance > 0:
            noise = self.rng.normal(0, self.config.solar_noise_std * base_irradiance)
        else:
            noise = 0.0
            
        total_irradiance = max(0, base_irradiance + noise)
        
        # Ensure physical constraints
        total_irradiance = min(total_irradiance, self.config.solar_max_irradiance)
        
        return {
            "irradiance": total_irradiance,
            "direct": max(0, direct_irradiance + (noise * 0.7 if base_irradiance > 0 else 0)),
            "diffuse": max(0, diffuse_irradiance + (noise * 0.3 if base_irradiance > 0 else 0)),
            "elevation": elevation,
            "azimuth": azimuth,
            "cloud_cover": cloud_cover
        }
    
    def _update_cloud_state(self) -> float:
        """Update cloud state with realistic temporal patterns"""
        if self.cloud_duration_remaining <= 0:
            # Decide if new cloud event starts
            if self.rng.random() < self.config.cloud_probability / 3600:  # Per second probability
                self.cloud_duration_remaining = self.rng.exponential(self.config.cloud_duration_mean * 60)
                self.cloud_state = self.rng.uniform(0.3, 1.0)  # Cloud intensity
            else:
                self.cloud_state = max(0, self.cloud_state - 0.01)  # Gradual clearing
        else:
            self.cloud_duration_remaining -= self.config.sampling_interval
            # Add some variation to cloud intensity
            self.cloud_state += self.rng.normal(0, 0.05)
            self.cloud_state = max(0, min(1, self.cloud_state))
        
        return self.cloud_state
    
    def generate_wind_data(self, timestamp: datetime) -> Dict[str, float]:
        """
        Generate realistic wind speed and direction with turbulence
        """
        # Base wind speed from Weibull distribution
        base_wind_speed = self.rng.weibull(self.config.wind_weibull_shape) * self.config.wind_weibull_scale
        
        # Add temporal correlation to previous wind speed
        self.wind_speed_state = 0.9 * self.wind_speed_state + 0.1 * base_wind_speed
        
        # Add turbulence
        turbulence = self.rng.normal(0, self.config.wind_turbulence_std)
        wind_speed = max(0, self.wind_speed_state + turbulence)
        
        # Wind direction with some persistence
        direction_change = self.rng.normal(0, self.config.wind_direction_std)
        self.wind_direction_state = (self.wind_direction_state + direction_change) % 360
        
        # Gust events
        gust_speed = wind_speed
        turbulence_intensity = abs(turbulence) / max(wind_speed, 0.1)
        
        if self.rng.random() < self.config.wind_gust_probability / 3600:  # Per second probability
            gust_multiplier = self.rng.uniform(1.2, self.config.wind_gust_multiplier)
            gust_speed = wind_speed * gust_multiplier
            turbulence_intensity = min(1.0, turbulence_intensity * 1.5)
        
        return {
            "speed": wind_speed,
            "direction": self.wind_direction_state,
            "gust_speed": gust_speed,
            "turbulence_intensity": min(1.0, turbulence_intensity)
        }
    
    def generate_temperature(self, timestamp: datetime) -> Dict[str, float]:
        """
        Generate realistic temperature with diurnal and seasonal cycles
        """
        # Seasonal component
        day_of_year = timestamp.timetuple().tm_yday
        seasonal_temp = self.config.temp_annual_mean + self.config.temp_annual_amplitude * \
                       math.sin(2 * math.pi * (day_of_year - 80) / 365)
        
        # Diurnal component
        hour = timestamp.hour + timestamp.minute / 60.0
        diurnal_temp = self.config.temp_diurnal_amplitude * \
                      math.sin(2 * math.pi * (hour - 6) / 24)
        
        # Temperature correlation with cloud cover
        cloud_effect = -2.0 * self.cloud_state  # Clouds reduce temperature
        
        # Base temperature
        base_temp = seasonal_temp + diurnal_temp + cloud_effect
        
        # Add temporal correlation
        self.temperature_state = 0.95 * self.temperature_state + 0.05 * base_temp
        
        # Add noise
        noise = self.rng.normal(0, self.config.temp_noise_std)
        temperature = self.temperature_state + noise
        
        # Calculate derived values
        dew_point = temperature - 5.0 - self.rng.uniform(0, 10)  # Simplified dew point
        humidity = max(20, min(100, 80 - (temperature - 20) * 2 + self.rng.uniform(-10, 10)))
        pressure = 1013.25 + self.rng.normal(0, 5)  # Standard pressure with variation
        
        return {
            "temperature": temperature,
            "dew_point": dew_point,
            "humidity": humidity,
            "pressure": pressure
        }
    
    def calculate_power_potential(self, solar_data: Dict, wind_data: Dict) -> Dict[str, float]:
        """
        Calculate potential power generation from weather data
        """
        # Solar power potential (assuming 1MW solar farm)
        solar_power = (solar_data["irradiance"] / 1000.0) * 1000.0  # kW
        
        # Wind power potential (using simplified power curve)
        wind_speed = wind_data["speed"]
        if wind_speed < 3:  # Cut-in speed
            wind_power = 0
        elif wind_speed < 12:  # Rated speed
            wind_power = 2000 * (wind_speed / 12) ** 3  # kW
        elif wind_speed < 25:  # Cut-out speed
            wind_power = 2000  # kW (rated power)
        else:
            wind_power = 0  # Cut-out
        
        return {
            "solar_power_potential": solar_power,
            "wind_power_potential": wind_power
        }
    
    def generate_weather_data(self, timestamp: Optional[datetime] = None) -> WeatherData:
        """
        Generate complete weather data point
        """
        if timestamp is None:
            timestamp = self.current_time
            self.current_time += timedelta(seconds=self.config.sampling_interval)
        
        # Generate all weather components
        solar_data = self.generate_solar_irradiance(timestamp)
        wind_data = self.generate_wind_data(timestamp)
        temp_data = self.generate_temperature(timestamp)
        power_data = self.calculate_power_potential(solar_data, wind_data)
        
        # Data quality simulation
        data_quality = 1.0
        missing_flags = {}
        
        # Simulate occasional data quality issues
        if self.rng.random() < self.config.missing_data_probability:
            data_quality = 0.0
            missing_flags = {
                "solar_irradiance": True,
                "wind_speed": True,
                "temperature": True
            }
        
        # Create weather data object
        weather_data = WeatherData(
            timestamp=timestamp,
            location_id=self.location_id,
            latitude=self.config.latitude,
            longitude=self.config.longitude,
            
            solar_irradiance=solar_data["irradiance"],
            solar_irradiance_diffuse=solar_data["diffuse"],
            solar_irradiance_direct=solar_data["direct"],
            solar_elevation_angle=solar_data["elevation"],
            solar_azimuth_angle=solar_data["azimuth"],
            cloud_cover=solar_data["cloud_cover"],
            
            wind_speed=wind_data["speed"],
            wind_direction=wind_data["direction"],
            wind_gust_speed=wind_data["gust_speed"],
            wind_turbulence_intensity=wind_data["turbulence_intensity"],
            
            temperature=temp_data["temperature"],
            temperature_dew_point=temp_data["dew_point"],
            humidity=temp_data["humidity"],
            pressure=temp_data["pressure"],
            
            data_quality_score=data_quality,
            missing_data_flags=missing_flags,
            
            solar_power_potential=power_data["solar_power_potential"],
            wind_power_potential=power_data["wind_power_potential"],
            weather_forecast_confidence=0.95  # High confidence for current conditions
        )
        
        return weather_data
    
    def generate_batch(self, batch_size: int = None) -> List[WeatherData]:
        """
        Generate a batch of weather data points
        """
        if batch_size is None:
            batch_size = self.config.batch_size
        
        batch = []
        for _ in range(batch_size):
            weather_data = self.generate_weather_data()
            batch.append(weather_data)
        
        return batch