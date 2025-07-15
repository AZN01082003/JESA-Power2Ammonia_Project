# config/postgres/init.sql

-- Power-to-Ammonia Database Initialization Script

-- Create database and users
CREATE DATABASE IF NOT EXISTS power2ammonia;
CREATE USER IF NOT EXISTS power2ammonia WITH PASSWORD 'power2ammonia_password';
GRANT ALL PRIVILEGES ON DATABASE power2ammonia TO power2ammonia;

-- Connect to the database
\c power2ammonia;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS equipment;
CREATE SCHEMA IF NOT EXISTS weather;
CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Equipment table
CREATE TABLE IF NOT EXISTS equipment.equipment_registry (
    id SERIAL PRIMARY KEY,
    equipment_id VARCHAR(100) UNIQUE NOT NULL,
    equipment_type VARCHAR(50) NOT NULL,
    location_id VARCHAR(50) NOT NULL,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    commissioned_date DATE,
    rated_capacity DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather stations
CREATE TABLE IF NOT EXISTS weather.weather_stations (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(50) UNIQUE NOT NULL,
    station_name VARCHAR(100),
    latitude DECIMAL(8,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    elevation DECIMAL(8,2),
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Production batches
CREATE TABLE IF NOT EXISTS production.production_batches (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) UNIQUE NOT NULL,
    plant_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    target_nh3_production DECIMAL(10,2),
    actual_nh3_production DECIMAL(10,2),
    efficiency DECIMAL(5,4),
    status VARCHAR(20) DEFAULT 'running',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Monitoring alerts
CREATE TABLE IF NOT EXISTS monitoring.alerts (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR(100) UNIQUE NOT NULL,
    source_id VARCHAR(100) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    acknowledged BOOLEAN DEFAULT FALSE,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_equipment_type ON equipment.equipment_registry(equipment_type);
CREATE INDEX IF NOT EXISTS idx_equipment_location ON equipment.equipment_registry(location_id);
CREATE INDEX IF NOT EXISTS idx_production_plant ON production.production_batches(plant_id);
CREATE INDEX IF NOT EXISTS idx_production_status ON production.production_batches(status);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON monitoring.alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON monitoring.alerts(resolved) WHERE resolved = FALSE;

-- Insert sample data
INSERT INTO equipment.equipment_registry (
    equipment_id, equipment_type, location_id, manufacturer, model, 
    commissioned_date, rated_capacity, status
) VALUES 
    ('plant_001_electrolyzer_001', 'electrolyzer', 'casablanca_001', 'HydrogenTech', 'HT-10MW', '2024-01-15', 10.00, 'active'),
    ('plant_001_asu_001', 'asu', 'casablanca_001', 'AirSep', 'AS-500', '2024-01-15', 500.00, 'active'),
    ('plant_001_haber_bosch_001', 'haber_bosch', 'casablanca_001', 'AmmoniaWorks', 'AW-100', '2024-01-15', 100.00, 'active'),
    ('plant_001_battery_001', 'battery', 'casablanca_001', 'BatteryTech', 'BT-50MWh', '2024-01-15', 50.00, 'active'),
    ('plant_001_h2_storage_001', 'h2_storage', 'casablanca_001', 'HydrogenStorage', 'HS-1000', '2024-01-15', 1000.00, 'active')
ON CONFLICT (equipment_id) DO NOTHING;

INSERT INTO weather.weather_stations (
    location_id, station_name, latitude, longitude, elevation, timezone
) VALUES 
    ('casablanca_001', 'Casablanca Power Plant Weather Station', 33.5731, -7.5898, 56, 'Africa/Casablanca')
ON CONFLICT (location_id) DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA equipment TO power2ammonia;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA weather TO power2ammonia;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA production TO power2ammonia;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA monitoring TO power2ammonia;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA equipment TO power2ammonia;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA weather TO power2ammonia;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA production TO power2ammonia;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA monitoring TO power2ammonia;
