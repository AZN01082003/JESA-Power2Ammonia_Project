# schemas/weather_schema.py

from dataclasses import dataclass, asdict
from typing import Dict, Optional, Any
from datetime import datetime
import json

@dataclass
class WeatherData:
    """Schema for weather data messages"""
    
    # Metadata
    timestamp: datetime
    location_id: str
    latitude: float
    longitude: float
    
    # Solar data
    solar_irradiance: float           # W/m²
    solar_irradiance_diffuse: float   # W/m² (diffuse component)
    solar_irradiance_direct: float    # W/m² (direct component)
    solar_elevation_angle: float      # degrees
    solar_azimuth_angle: float        # degrees
    cloud_cover: float                # 0-1 (0=clear, 1=overcast)
    
    # Wind data
    wind_speed: float                 # m/s
    wind_direction: float             # degrees (0-360)
    wind_gust_speed: float            # m/s
    wind_turbulence_intensity: float  # 0-1
    
    # Temperature data
    temperature: float                # °C
    temperature_dew_point: float      # °C
    humidity: float                   # % (0-100)
    pressure: float                   # hPa
    
    # Data quality indicators
    data_quality_score: float         # 0-1 (1=perfect quality)
    missing_data_flags: Dict[str, bool]
    
    # Derived metrics (for energy estimation)
    solar_power_potential: float      # kW (estimated from irradiance)
    wind_power_potential: float       # kW (estimated from wind)
    weather_forecast_confidence: float # 0-1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        # Convert datetime to ISO string
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WeatherData':
        """Create from dictionary"""
        # Convert timestamp from string if needed
        if isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'WeatherData':
        """Create from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def validate(self) -> Dict[str, bool]:
        """Validate weather data ranges"""
        validations = {
            "solar_irradiance_range": 0 <= self.solar_irradiance <= 1500,
            "wind_speed_range": 0 <= self.wind_speed <= 50,
            "temperature_range": -30 <= self.temperature <= 60,
            "humidity_range": 0 <= self.humidity <= 100,
            "pressure_range": 900 <= self.pressure <= 1100,
            "cloud_cover_range": 0 <= self.cloud_cover <= 1,
            "wind_direction_range": 0 <= self.wind_direction <= 360,
            "quality_score_range": 0 <= self.data_quality_score <= 1,
        }
        return validations
    
    def is_valid(self) -> bool:
        """Check if all validations pass"""
        return all(self.validate().values())

@dataclass
class WeatherAlert:
    """Schema for weather-related alerts"""
    
    timestamp: datetime
    location_id: str
    alert_type: str              # "high_wind", "low_irradiance", "extreme_temp"
    severity: str                # "low", "medium", "high", "critical"
    message: str
    duration_minutes: Optional[int]
    threshold_value: float
    current_value: float
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

# Schema validation functions
def validate_weather_message(message: Dict[str, Any]) -> bool:
    """Validate incoming weather message structure"""
    required_fields = [
        'timestamp', 'location_id', 'solar_irradiance', 'wind_speed', 
        'temperature', 'humidity', 'pressure', 'cloud_cover'
    ]
    
    # Check required fields
    if not all(field in message for field in required_fields):
        return False
    
    # Basic range checks
    try:
        weather_data = WeatherData.from_dict(message)
        return weather_data.is_valid()
    except Exception:
        return False