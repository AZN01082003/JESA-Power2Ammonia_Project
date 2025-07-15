# test_weather_generator.py

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
import pandas as pd
import json
from data_producers.weather_generator import WeatherGenerator
from config.data_config import WEATHER_CONFIG

def test_weather_generator():
    """Test the weather generator functionality"""
    
    print("  Testing Weather Generator...")
    print("=" * 50)
    
    # Initialize generator
    generator = WeatherGenerator(location_id="test_location")
    
    # Test single data point generation
    print("\n1. Testing single weather data point generation:")
    weather_data = generator.generate_weather_data()
    print(f"   Timestamp: {weather_data.timestamp}")
    print(f"   Solar Irradiance: {weather_data.solar_irradiance:.2f} W/m²")
    print(f"   Wind Speed: {weather_data.wind_speed:.2f} m/s")
    print(f"   Temperature: {weather_data.temperature:.2f} °C")
    print(f"   Cloud Cover: {weather_data.cloud_cover:.2f}")
    print(f"   Solar Power Potential: {weather_data.solar_power_potential:.2f} kW")
    print(f"   Wind Power Potential: {weather_data.wind_power_potential:.2f} kW")
    
    # Test validation
    print("\n2. Testing data validation:")
    validations = weather_data.validate()
    all_valid = weather_data.is_valid()
    print(f"   All validations passed: {all_valid}")
    if not all_valid:
        for check, result in validations.items():
            if not result:
                print(f"    Failed: {check}")
    
    # Test JSON serialization
    print("\n3. Testing JSON serialization:")
    json_data = weather_data.to_json()
    print(f"   JSON length: {len(json_data)} characters")
    
    # Test deserialization
    deserialized = weather_data.from_json(json_data)
    print(f"   Deserialization successful: {deserialized.timestamp == weather_data.timestamp}")
    
    # Test batch generation
    print("\n4. Testing batch generation:")
    batch_size = 10
    batch = generator.generate_batch(batch_size)
    print(f"   Generated {len(batch)} data points")
    
    # Test time series generation
    print("\n5. Testing time series generation (24 hours):")
    start_time = datetime.now()
    time_series = []
    
    for i in range(24 * 60):  # 24 hours, 1 minute intervals
        generator.current_time = start_time + timedelta(minutes=i)
        weather_data = generator.generate_weather_data()
        time_series.append(weather_data)
    
    # Convert to DataFrame for analysis
    df_data = []
    for data in time_series:
        df_data.append({
            'timestamp': data.timestamp,
            'solar_irradiance': data.solar_irradiance,
            'wind_speed': data.wind_speed,
            'temperature': data.temperature,
            'cloud_cover': data.cloud_cover,
            'solar_power': data.solar_power_potential,
            'wind_power': data.wind_power_potential
        })
    
    df = pd.DataFrame(df_data)
    
    # Print statistics
    print(f"   Time series generated: {len(df)} points")
    print(f"   Solar irradiance - Mean: {df['solar_irradiance'].mean():.2f}, Max: {df['solar_irradiance'].max():.2f}")
    print(f"   Wind speed - Mean: {df['wind_speed'].mean():.2f}, Max: {df['wind_speed'].max():.2f}")
    print(f"   Temperature - Mean: {df['temperature'].mean():.2f}, Range: {df['temperature'].max()-df['temperature'].min():.2f}°C")
    print(f"   Total potential power - Mean: {(df['solar_power']+df['wind_power']).mean():.2f} kW")
    
    # Test diurnal patterns
    print("\n6. Testing diurnal patterns:")
    morning_solar = df[(df['timestamp'].dt.hour >= 6) & (df['timestamp'].dt.hour <= 8)]['solar_irradiance'].mean()
    noon_solar = df[(df['timestamp'].dt.hour >= 11) & (df['timestamp'].dt.hour <= 13)]['solar_irradiance'].mean()
    evening_solar = df[(df['timestamp'].dt.hour >= 18) & (df['timestamp'].dt.hour <= 20)]['solar_irradiance'].mean()
    
    print(f"   Morning solar (6-8h): {morning_solar:.2f} W/m²")
    print(f"   Noon solar (11-13h): {noon_solar:.2f} W/m²")
    print(f"   Evening solar (18-20h): {evening_solar:.2f} W/m²")
    print(f"   Diurnal pattern correct: {morning_solar < noon_solar > evening_solar}")
    
    # Test realistic ranges
    print("\n7. Testing realistic data ranges:")
    tests = [
        ("Solar irradiance", df['solar_irradiance'].min() >= 0 and df['solar_irradiance'].max() <= 1200),
        ("Wind speed", df['wind_speed'].min() >= 0 and df['wind_speed'].max() <= 30),
        ("Temperature", df['temperature'].min() >= -10 and df['temperature'].max() <= 50),
        ("Cloud cover", df['cloud_cover'].min() >= 0 and df['cloud_cover'].max() <= 1)
    ]
    
    for test_name, result in tests:
        print(f"   {test_name}: {' PASS' if result else ' FAIL'}")
    
    print("\n" + "=" * 50)
    print(" Weather Generator Test Complete!")
    
    return df

def demonstrate_weather_patterns():
    """Demonstrate different weather patterns"""
    print("\n  Demonstrating Weather Patterns...")
    print("=" * 50)
    
    generator = WeatherGenerator()
    
    # Test different times of day
    times = [
        ("Night", datetime(2024, 6, 15, 2, 0, 0)),
        ("Morning", datetime(2024, 6, 15, 8, 0, 0)),
        ("Noon", datetime(2024, 6, 15, 12, 0, 0)),
        ("Evening", datetime(2024, 6, 15, 18, 0, 0))
    ]
    
    for period, test_time in times:
        weather = generator.generate_weather_data(test_time)
        print(f"\n{period} ({test_time.strftime('%H:%M')}):")
        print(f"   Solar: {weather.solar_irradiance:.1f} W/m² (elevation: {weather.solar_elevation_angle:.1f}°)")
        print(f"   Wind: {weather.wind_speed:.1f} m/s")
        print(f"   Temperature: {weather.temperature:.1f}°C")
        print(f"   Power potential: {weather.solar_power_potential + weather.wind_power_potential:.1f} kW")

if __name__ == "__main__":
    # Run tests
    df = test_weather_generator()
    demonstrate_weather_patterns()
    
    # Optional: Save sample data
    print(f"\n Saving sample data to 'sample_weather_data.csv'...")
    df.to_csv('sample_weather_data.csv', index=False)
    print(" Sample data saved!")