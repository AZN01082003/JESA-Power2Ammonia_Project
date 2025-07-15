# data_producers/base_producer.py

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Callable
import json
import time
import threading
import logging
from datetime import datetime, timedelta
from dataclasses import asdict
import queue

# Kafka imports (will be used later)
# from kafka import KafkaProducer
# from kafka.errors import KafkaError

class BaseProducer(ABC):
    """
    Abstract base class for all data producers in the power-to-ammonia system.
    Provides common functionality for data generation, batching, and future Kafka integration.
    """
    
    def __init__(self, producer_id: str, location_id: str, batch_size: int = 100, 
                 sampling_interval: float = 1.0):
        self.producer_id = producer_id
        self.location_id = location_id
        self.batch_size = batch_size
        self.sampling_interval = sampling_interval
        
        # State management
        self.is_running = False
        self.is_paused = False
        self.current_time = datetime.now()
        self.start_time = None
        self.total_messages_sent = 0
        self.total_bytes_sent = 0
        
        # Batching and buffering
        self.message_buffer = queue.Queue(maxsize=1000)
        self.batch_buffer = []
        self.last_batch_time = time.time()
        
        # Threading
        self.producer_thread = None
        self.stop_event = threading.Event()
        
        # Callbacks and hooks
        self.on_message_generated = None
        self.on_batch_sent = None
        self.on_error = None
        
        # Kafka producer (will be initialized later)
        self.kafka_producer = None
        
        # Logging
        self.logger = logging.getLogger(f"producer.{producer_id}")
        self.logger.setLevel(logging.INFO)
        
        # Performance metrics
        self.metrics = {
            'messages_per_second': 0.0,
            'bytes_per_second': 0.0,
            'average_message_size': 0.0,
            'buffer_utilization': 0.0,
            'error_count': 0,
            'last_message_time': None
        }
    
    @abstractmethod
    def generate_data(self, timestamp: Optional[datetime] = None) -> Any:
        """
        Generate a single data point. Must be implemented by subclasses.
        
        Args:
            timestamp: Optional timestamp for the data point
            
        Returns:
            Data object (should be JSON serializable)
        """
        pass
    
    @abstractmethod
    def get_topic_name(self) -> str:
        """
        Get the Kafka topic name for this producer.
        
        Returns:
            Topic name string
        """
        pass
    
    @abstractmethod
    def serialize_data(self, data: Any) -> Dict[str, Any]:
        """
        Serialize data object to dictionary for JSON serialization.
        
        Args:
            data: Data object to serialize
            
        Returns:
            Dictionary representation of the data
        """
        pass
    
    def setup_kafka_producer(self, bootstrap_servers: List[str], 
                           kafka_config: Optional[Dict[str, Any]] = None):
        """
        Setup Kafka producer (placeholder for future implementation)
        
        Args:
            bootstrap_servers: List of Kafka bootstrap servers
            kafka_config: Additional Kafka configuration
        """
        self.logger.info(f"Setting up Kafka producer for {self.producer_id}")
        
        # Placeholder - will be implemented when we add Kafka
        # default_config = {
        #     'bootstrap_servers': bootstrap_servers,
        #     'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
        #     'key_serializer': lambda k: k.encode('utf-8') if k else None,
        #     'acks': 'all',
        #     'retries': 3,
        #     'batch_size': 16384,
        #     'linger_ms': 10,
        #     'buffer_memory': 33554432
        # }
        
        # if kafka_config:
        #     default_config.update(kafka_config)
        
        # self.kafka_producer = KafkaProducer(**default_config)
        pass
    
    def create_message(self, data: Any) -> Dict[str, Any]:
        """
        Create a standardized message from data
        
        Args:
            data: Data object to wrap in message
            
        Returns:
            Standardized message dictionary
        """
        serialized_data = self.serialize_data(data)
        
        message = {
            'producer_id': self.producer_id,
            'location_id': self.location_id,
            'timestamp': datetime.now().isoformat(),
            'message_id': f"{self.producer_id}_{self.total_messages_sent}",
            'data': serialized_data,
            'metadata': {
                'version': '1.0',
                'schema_version': '1.0',
                'producer_type': self.__class__.__name__
            }
        }
        
        return message
    
    def add_to_batch(self, message: Dict[str, Any]):
        """
        Add message to batch buffer
        
        Args:
            message: Message to add to batch
        """
        self.batch_buffer.append(message)
        
        # Check if batch is full or time limit reached
        current_time = time.time()
        batch_timeout = 5.0  # seconds
        
        if (len(self.batch_buffer) >= self.batch_size or 
            current_time - self.last_batch_time > batch_timeout):
            self.send_batch()
    
    def send_batch(self):
        """
        Send current batch of messages
        """
        if not self.batch_buffer:
            return
        
        batch = {
            'batch_id': f"{self.producer_id}_{int(time.time())}",
            'producer_id': self.producer_id,
            'timestamp': datetime.now().isoformat(),
            'message_count': len(self.batch_buffer),
            'messages': self.batch_buffer.copy()
        }
        
        # Send to Kafka (placeholder)
        self.send_to_kafka(batch)
        
        # Update metrics
        self.total_messages_sent += len(self.batch_buffer)
        batch_size_bytes = len(json.dumps(batch))
        self.total_bytes_sent += batch_size_bytes
        
        # Call callback if set
        if self.on_batch_sent:
            self.on_batch_sent(batch)
        
        # Clear batch buffer
        self.batch_buffer.clear()
        self.last_batch_time = time.time()
        
        self.logger.info(f"Sent batch with {len(batch['messages'])} messages")
    
    def send_to_kafka(self, batch: Dict[str, Any]):
        """
        Send batch to Kafka topic (placeholder)
        
        Args:
            batch: Batch of messages to send
        """
        topic = self.get_topic_name()
        
        # Placeholder for Kafka sending
        # if self.kafka_producer:
        #     try:
        #         future = self.kafka_producer.send(topic, batch)
        #         future.get(timeout=10)  # Wait for send to complete
        #     except KafkaError as e:
        #         self.logger.error(f"Error sending to Kafka: {e}")
        #         if self.on_error:
        #             self.on_error(e)
        # else:
        #     self.logger.warning("Kafka producer not configured, batch not sent")
        
        # For now, just log the batch
        self.logger.info(f"Batch sent to topic '{topic}' with {len(batch['messages'])} messages")
    
    def generate_single_message(self) -> Dict[str, Any]:
        """
        Generate single message
        
        Returns:
            Generated message
        """
        # Generate data
        data = self.generate_data(self.current_time)
        
        # Create message
        message = self.create_message(data)
        
        # Call callback if set
        if self.on_message_generated:
            self.on_message_generated(message)
        
        # Update time
        self.current_time += timedelta(seconds=self.sampling_interval)
        
        return message
    
    def producer_loop(self):
        """
        Main producer loop (runs in separate thread)
        """
        self.logger.info(f"Starting producer loop for {self.producer_id}")
        
        while not self.stop_event.is_set():
            try:
                if not self.is_paused:
                    # Generate and send message
                    message = self.generate_single_message()
                    self.add_to_batch(message)
                    
                    # Update metrics
                    self.update_metrics()
                
                # Wait for next sampling interval
                time.sleep(self.sampling_interval)
                
            except Exception as e:
                self.logger.error(f"Error in producer loop: {e}")
                self.metrics['error_count'] += 1
                
                if self.on_error:
                    self.on_error(e)
                
                # Continue after error
                time.sleep(1.0)
    
    def start(self):
        """
        Start the producer
        """
        if self.is_running:
            self.logger.warning(f"Producer {self.producer_id} is already running")
            return
        
        self.logger.info(f"Starting producer {self.producer_id}")
        
        self.is_running = True
        self.start_time = datetime.now()
        self.stop_event.clear()
        
        # Start producer thread
        self.producer_thread = threading.Thread(target=self.producer_loop)
        self.producer_thread.daemon = True
        self.producer_thread.start()
        
        self.logger.info(f"Producer {self.producer_id} started successfully")
    
    def stop(self):
        """
        Stop the producer
        """
        if not self.is_running:
            self.logger.warning(f"Producer {self.producer_id} is not running")
            return
        
        self.logger.info(f"Stopping producer {self.producer_id}")
        
        self.is_running = False
        self.stop_event.set()
        
        # Send any remaining messages in batch
        self.send_batch()
        
        # Wait for thread to finish
        if self.producer_thread and self.producer_thread.is_alive():
            self.producer_thread.join(timeout=5.0)
        
        # Close Kafka producer
        if self.kafka_producer:
            self.kafka_producer.close()
        
        self.logger.info(f"Producer {self.producer_id} stopped")
    
    def pause(self):
        """
        Pause the producer
        """
        self.is_paused = True
        self.logger.info(f"Producer {self.producer_id} paused")
    
    def resume(self):
        """
        Resume the producer
        """
        self.is_paused = False
        self.logger.info(f"Producer {self.producer_id} resumed")
    
    def update_metrics(self):
        """
        Update performance metrics
        """
        current_time = time.time()
        
        if self.start_time:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            
            if elapsed_time > 0:
                self.metrics['messages_per_second'] = self.total_messages_sent / elapsed_time
                self.metrics['bytes_per_second'] = self.total_bytes_sent / elapsed_time
                
                if self.total_messages_sent > 0:
                    self.metrics['average_message_size'] = self.total_bytes_sent / self.total_messages_sent
        
        # Buffer utilization
        self.metrics['buffer_utilization'] = len(self.batch_buffer) / self.batch_size
        self.metrics['last_message_time'] = current_time
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current producer metrics
        
        Returns:
            Dictionary of metrics
        """
        return {
            'producer_id': self.producer_id,
            'is_running': self.is_running,
            'is_paused': self.is_paused,
            'total_messages_sent': self.total_messages_sent,
            'total_bytes_sent': self.total_bytes_sent,
            'runtime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            'performance_metrics': self.metrics.copy()
        }
    
    def set_callbacks(self, on_message_generated: Optional[Callable] = None,
                     on_batch_sent: Optional[Callable] = None,
                     on_error: Optional[Callable] = None):
        """
        Set callback functions
        
        Args:
            on_message_generated: Callback for when message is generated
            on_batch_sent: Callback for when batch is sent
            on_error: Callback for when error occurs
        """
        self.on_message_generated = on_message_generated
        self.on_batch_sent = on_batch_sent
        self.on_error = on_error
    
    def generate_batch(self, batch_size: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Generate a batch of messages (synchronous)
        
        Args:
            batch_size: Override default batch size
            
        Returns:
            List of generated messages
        """
        size = batch_size or self.batch_size
        messages = []
        
        for _ in range(size):
            message = self.generate_single_message()
            messages.append(message)
        
        return messages
    
    def __enter__(self):
        """
        Context manager entry
        """
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit
        """
        self.stop()

# Example implementation for Weather Producer
class WeatherProducer(BaseProducer):
    """
    Example weather data producer implementation
    """
    
    def __init__(self, producer_id: str, location_id: str, **kwargs):
        super().__init__(producer_id, location_id, **kwargs)
        
        # Import here to avoid circular imports
        from data_producers.weather_generator import WeatherGenerator
        self.weather_generator = WeatherGenerator(location_id=location_id)
    
    def generate_data(self, timestamp: Optional[datetime] = None) -> Any:
        """
        Generate weather data
        """
        return self.weather_generator.generate_weather_data(timestamp)
    
    def get_topic_name(self) -> str:
        """
        Get topic name for weather data
        """
        return "weather-raw"
    
    def serialize_data(self, data: Any) -> Dict[str, Any]:
        """
        Serialize weather data
        """
        return data.to_dict()

# Example usage and testing
if __name__ == "__main__":
    # Test the base producer with weather data
    print("🧪 Testing Base Producer with Weather Data...")
    
    # Create weather producer
    weather_producer = WeatherProducer(
        producer_id="weather_001",
        location_id="casablanca_001",
        batch_size=10,
        sampling_interval=1.0
    )
    
    # Set up callbacks
    def on_message_generated(message):
        print(f"Generated message: {message['message_id']}")
    
    def on_batch_sent(batch):
        print(f"Sent batch: {batch['batch_id']} with {batch['message_count']} messages")
    
    def on_error(error):
        print(f"Error occurred: {error}")
    
    weather_producer.set_callbacks(on_message_generated, on_batch_sent, on_error)
    
    # Test synchronous batch generation
    print("\n1. Testing synchronous batch generation:")
    batch = weather_producer.generate_batch(5)
    print(f"Generated {len(batch)} messages")
    
    # Test asynchronous producer
    print("\n2. Testing asynchronous producer:")
    
    with weather_producer:
        # Let it run for 15 seconds
        time.sleep(15)
        
        # Check metrics
        metrics = weather_producer.get_metrics()
        print(f"Metrics: {metrics}")
    
    print("\n✅ Base Producer test completed!")