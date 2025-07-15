# data_producers/kafka_equipment_producers.py

import json
import logging
import time
import threading
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import asdict

from data_producers.base_producer import BaseProducer
from data_producers.weather_generator import WeatherGenerator
from data_producers.electrolyzer_simulator import ElectrolyzerSimulator
from data_producers.asu_simulator import ASUSimulator
from data_producers.haber_bosch_simulator import HaberBoschSimulator
from data_producers.energy_storage_simulators import BatteryStorageSimulator, H2StorageSimulator
from data_producers.equipment_orchestrator import EquipmentOrchestrator
from utils.kafka_utils import KafkaProducerWrapper

class WeatherKafkaProducer(BaseProducer):
    """
    Kafka producer for weather data
    """
    
    def __init__(self, producer_id: str, location_id: str, 
                 bootstrap_servers: List[str] = None, **kwargs):
        super().__init__(producer_id, location_id, **kwargs)
        
        self.weather_generator = WeatherGenerator(location_id=location_id)
        self.kafka_producer = KafkaProducerWrapper(bootstrap_servers)
        
    def generate_data(self, timestamp: Optional[datetime] = None) -> Any:
        """Generate weather data"""
        return self.weather_generator.generate_weather_data(timestamp)
    
    def get_topic_name(self) -> str:
        """Get topic name for weather data"""
        return "weather-raw"
    
    def serialize_data(self, data: Any) -> Dict[str, Any]:
        """Serialize weather data"""
        return data.to_dict()
    
    def send_to_kafka(self, batch: Dict[str, Any]):
        """Send batch to Kafka topic"""
        topic = self.get_topic_name()
        
        if self.kafka_producer and self.kafka_producer.producer:
            try:
                # Send individual messages from batch
                for message in batch['messages']:
                    # Use location_id as key for partitioning
                    key = f"{self.location_id}_{message['data'].get('location_id', '')}"
                    success = self.kafka_producer.send_message(topic, message, key)
                    
                    if not success:
                        self.logger.error(f"Failed to send weather message to Kafka")
                
                self.logger.info(f"Sent weather batch to Kafka topic '{topic}'")
                
            except Exception as e:
                self.logger.error(f"Error sending weather batch to Kafka: {e}")
                if self.on_error:
                    self.on_error(e)
        else:
            self.logger.warning("Kafka producer not available for weather data")

class EquipmentKafkaProducer(BaseProducer):
    """
    Kafka producer for individual equipment telemetry
    """
    
    def __init__(self, producer_id: str, equipment_type: str, equipment_id: str,
                 location_id: str, bootstrap_servers: List[str] = None, **kwargs):
        super().__init__(producer_id, location_id, **kwargs)
        
        self.equipment_type = equipment_type
        self.equipment_id = equipment_id
        self.kafka_producer = KafkaProducerWrapper(bootstrap_servers)
        
        # Initialize appropriate equipment simulator
        if equipment_type == "electrolyzer":
            self.equipment = ElectrolyzerSimulator(equipment_id, location_id=location_id)
            self.power_demand = 8.0  # Default MW
        elif equipment_type == "asu":
            self.equipment = ASUSimulator(equipment_id, location_id=location_id)
            self.n2_demand = 400.0  # Default Nm³/h
        elif equipment_type == "haber_bosch":
            self.equipment = HaberBoschSimulator(equipment_id, location_id=location_id)
            self.nh3_target = 4.0  # Default t/h
            self.n2_available = 500.0  # Default kg/h
            self.h2_available = 200.0  # Default kg/h
        elif equipment_type == "battery":
            self.equipment = BatteryStorageSimulator(equipment_id, location_id=location_id)
            self.power_flow = 0.0  # Default MW (positive=charge, negative=discharge)
        elif equipment_type == "h2_storage":
            self.equipment = H2StorageSimulator(equipment_id, location_id=location_id)
            self.h2_input = 0.0  # Default kg/h
            self.h2_output = 0.0  # Default kg/h
        else:
            raise ValueError(f"Unknown equipment type: {equipment_type}")
    
    def generate_data(self, timestamp: Optional[datetime] = None) -> Any:
        """Generate equipment telemetry data"""
        # Step the equipment simulator
        if self.equipment_type == "electrolyzer":
            return self.equipment.step(self.power_demand, 25.0, self.sampling_interval / 3600)
        elif self.equipment_type == "asu":
            return self.equipment.step(self.n2_demand, 0.0, self.sampling_interval / 3600)
        elif self.equipment_type == "haber_bosch":
            return self.equipment.step(self.nh3_target, self.n2_available, 
                                     self.h2_available, self.sampling_interval / 3600)
        elif self.equipment_type == "battery":
            return self.equipment.step(self.power_flow, 25.0, self.sampling_interval / 3600)
        elif self.equipment_type == "h2_storage":
            return self.equipment.step(self.h2_input, self.h2_output, 25.0, 
                                     self.sampling_interval / 3600)
    
    def get_topic_name(self) -> str:
        """Get topic name for equipment telemetry"""
        return "equipment-telemetry"
    
    def serialize_data(self, data: Any) -> Dict[str, Any]:
        """Serialize equipment data"""
        return data.to_dict()
    
    def send_to_kafka(self, batch: Dict[str, Any]):
        """Send batch to Kafka topic"""
        topic = self.get_topic_name()
        
        if self.kafka_producer and self.kafka_producer.producer:
            try:
                for message in batch['messages']:
                    # Use equipment_id as key for partitioning
                    key = f"{self.equipment_type}_{self.equipment_id}"
                    success = self.kafka_producer.send_message(topic, message, key)
                    
                    if not success:
                        self.logger.error(f"Failed to send {self.equipment_type} message to Kafka")
                
                self.logger.info(f"Sent {self.equipment_type} batch to Kafka topic '{topic}'")
                
            except Exception as e:
                self.logger.error(f"Error sending {self.equipment_type} batch to Kafka: {e}")
                if self.on_error:
                    self.on_error(e)
        else:
            self.logger.warning(f"Kafka producer not available for {self.equipment_type}")
    
    def update_control_parameters(self, **kwargs):
        """Update equipment control parameters"""
        if self.equipment_type == "electrolyzer" and "power_demand" in kwargs:
            self.power_demand = kwargs["power_demand"]
        elif self.equipment_type == "asu" and "n2_demand" in kwargs:
            self.n2_demand = kwargs["n2_demand"]
        elif self.equipment_type == "haber_bosch":
            if "nh3_target" in kwargs:
                self.nh3_target = kwargs["nh3_target"]
            if "n2_available" in kwargs:
                self.n2_available = kwargs["n2_available"]
            if "h2_available" in kwargs:
                self.h2_available = kwargs["h2_available"]
        elif self.equipment_type == "battery" and "power_flow" in kwargs:
            self.power_flow = kwargs["power_flow"]
        elif self.equipment_type == "h2_storage":
            if "h2_input" in kwargs:
                self.h2_input = kwargs["h2_input"]
            if "h2_output" in kwargs:
                self.h2_output = kwargs["h2_output"]

class SystemKafkaProducer(BaseProducer):
    """
    Kafka producer for system-wide orchestrated data
    """
    
    def __init__(self, producer_id: str, plant_id: str, location_id: str,
                 bootstrap_servers: List[str] = None, **kwargs):
        super().__init__(producer_id, location_id, **kwargs)
        
        self.plant_id = plant_id
        self.orchestrator = EquipmentOrchestrator(plant_id, location_id)
        self.kafka_producer = KafkaProducerWrapper(bootstrap_servers)
        
        # System parameters
        self.nh3_target = 4.0  # t/h
        
        # Topic routing
        self.topic_mapping = {
            'weather': 'weather-processed',
            'system_overview': 'system-overview',
            'electrolyzer': 'equipment-telemetry',
            'asu': 'equipment-telemetry', 
            'haber_bosch': 'equipment-telemetry',
            'battery': 'storage-levels',
            'h2_storage': 'storage-levels',
            'power_flows': 'energy-consumption',
            'material_flows': 'production-metrics'
        }
    
    def generate_data(self, timestamp: Optional[datetime] = None) -> Any:
        """Generate complete system data"""
        return self.orchestrator.step(self.nh3_target, self.sampling_interval / 3600)
    
    def get_topic_name(self) -> str:
        """Get primary topic name"""
        return "system-overview"
    
    def serialize_data(self, data: Any) -> Dict[str, Any]:
        """Serialize system data"""
        # Convert all data objects to dictionaries
        serialized = {}
        for key, value in data.items():
            if hasattr(value, 'to_dict'):
                serialized[key] = value.to_dict()
            elif hasattr(value, '__dict__'):
                serialized[key] = asdict(value) if hasattr(value, '__dataclass_fields__') else value.__dict__
            else:
                serialized[key] = value
        return serialized
    
    def send_to_kafka(self, batch: Dict[str, Any]):
        """Send batch to multiple Kafka topics based on data type"""
        if not (self.kafka_producer and self.kafka_producer.producer):
            self.logger.warning("Kafka producer not available for system data")
            return
        
        try:
            for message in batch['messages']:
                system_data = message['data']
                
                # Route different data types to appropriate topics
                for data_type, topic in self.topic_mapping.items():
                    if data_type in system_data:
                        # Create topic-specific message
                        topic_message = {
                            'producer_id': self.producer_id,
                            'timestamp': message['timestamp'],
                            'message_id': f"{message['message_id']}_{data_type}",
                            'data_type': data_type,
                            'data': system_data[data_type],
                            'metadata': message['metadata']
                        }
                        
                        # Use plant_id as key
                        key = f"{self.plant_id}_{data_type}"
                        success = self.kafka_producer.send_message(topic, topic_message, key)
                        
                        if success:
                            self.logger.debug(f"Sent {data_type} data to topic '{topic}'")
                        else:
                            self.logger.error(f"Failed to send {data_type} data to topic '{topic}'")
            
            self.logger.info(f"Processed system batch with {len(batch['messages'])} messages")
            
        except Exception as e:
            self.logger.error(f"Error sending system batch to Kafka: {e}")
            if self.on_error:
                self.on_error(e)
    
    def update_system_parameters(self, nh3_target: Optional[float] = None):
        """Update system control parameters"""
        if nh3_target is not None:
            self.nh3_target = nh3_target

class ProducerManager:
    """
    Manages multiple Kafka producers for the power-to-ammonia system
    """
    
    def __init__(self, bootstrap_servers: List[str] = None):
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.producers = {}
        self.logger = logging.getLogger("kafka.producer_manager")
        
    def create_weather_producer(self, location_id: str, **kwargs) -> WeatherKafkaProducer:
        """Create and register weather producer"""
        producer_id = f"weather_{location_id}"
        producer = WeatherKafkaProducer(
            producer_id=producer_id,
            location_id=location_id,
            bootstrap_servers=self.bootstrap_servers,
            **kwargs
        )
        self.producers[producer_id] = producer
        return producer
    
    def create_equipment_producer(self, equipment_type: str, equipment_id: str, 
                                location_id: str, **kwargs) -> EquipmentKafkaProducer:
        """Create and register equipment producer"""
        producer_id = f"{equipment_type}_{equipment_id}"
        producer = EquipmentKafkaProducer(
            producer_id=producer_id,
            equipment_type=equipment_type,
            equipment_id=equipment_id,
            location_id=location_id,
            bootstrap_servers=self.bootstrap_servers,
            **kwargs
        )
        self.producers[producer_id] = producer
        return producer
    
    def create_system_producer(self, plant_id: str, location_id: str, **kwargs) -> SystemKafkaProducer:
        """Create and register system producer"""
        producer_id = f"system_{plant_id}"
        producer = SystemKafkaProducer(
            producer_id=producer_id,
            plant_id=plant_id,
            location_id=location_id,
            bootstrap_servers=self.bootstrap_servers,
            **kwargs
        )
        self.producers[producer_id] = producer
        return producer
    
    def start_all_producers(self):
        """Start all registered producers"""
        self.logger.info(f"Starting {len(self.producers)} producers...")
        
        for producer_id, producer in self.producers.items():
            try:
                producer.start()
                self.logger.info(f" Started producer: {producer_id}")
            except Exception as e:
                self.logger.error(f" Failed to start producer {producer_id}: {e}")
    
    def stop_all_producers(self):
        """Stop all registered producers"""
        self.logger.info(f"Stopping {len(self.producers)} producers...")
        
        for producer_id, producer in self.producers.items():
            try:
                producer.stop()
                self.logger.info(f" Stopped producer: {producer_id}")
            except Exception as e:
                self.logger.error(f" Failed to stop producer {producer_id}: {e}")
    
    def get_producer_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all producers"""
        metrics = {}
        for producer_id, producer in self.producers.items():
            metrics[producer_id] = producer.get_metrics()
        return metrics
    
    def get_producer(self, producer_id: str) -> Optional[BaseProducer]:
        """Get specific producer by ID"""
        return self.producers.get(producer_id)

def setup_full_producer_fleet(plant_id: str = "plant_001", location_id: str = "casablanca_001",
                             bootstrap_servers: List[str] = None) -> ProducerManager:
    """
    Setup complete fleet of producers for power-to-ammonia system
    """
    logger = logging.getLogger("kafka.fleet_setup")
    logger.info(" Setting up full producer fleet...")
    
    # Initialize producer manager
    manager = ProducerManager(bootstrap_servers)
    
    # Create weather producer
    weather_producer = manager.create_weather_producer(
        location_id=location_id,
        sampling_interval=10.0,  # 10 seconds
        batch_size=50
    )
    
    # Create equipment producers
    equipment_configs = [
        ("electrolyzer", f"{plant_id}_electrolyzer_001"),
        ("asu", f"{plant_id}_asu_001"),
        ("haber_bosch", f"{plant_id}_haber_bosch_001"),
        ("battery", f"{plant_id}_battery_001"),
        ("h2_storage", f"{plant_id}_h2_storage_001")
    ]
    
    for equipment_type, equipment_id in equipment_configs:
        equipment_producer = manager.create_equipment_producer(
            equipment_type=equipment_type,
            equipment_id=equipment_id,
            location_id=location_id,
            sampling_interval=30.0,  # 30 seconds
            batch_size=20
        )
    
    # Create system producer
    system_producer = manager.create_system_producer(
        plant_id=plant_id,
        location_id=location_id,
        sampling_interval=60.0,  # 1 minute
        batch_size=10
    )
    
    logger.info(f" Producer fleet setup complete: {len(manager.producers)} producers")
    return manager

if __name__ == "__main__":
    # Test the producer fleet
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print(" Testing Kafka Producer Fleet...")
    
    # Setup producer fleet
    manager = setup_full_producer_fleet()
    
    # Start producers
    manager.start_all_producers()
    
    # Let them run for 30 seconds
    print("Running producers for 30 seconds...")
    time.sleep(30)
    
    # Get metrics
    metrics = manager.get_producer_metrics()
    print("\n Producer Metrics:")
    for producer_id, metric in metrics.items():
        print(f"  {producer_id}: {metric['total_messages_sent']} messages, "
              f"{metric['performance_metrics']['messages_per_second']:.1f} msg/s")
    
    # Stop producers
    manager.stop_all_producers()
    
    print(" Producer fleet test completed!")