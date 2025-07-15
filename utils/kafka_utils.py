# utils/kafka_utils.py

import json
import logging
import time
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from dataclasses import asdict

try:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
    from kafka.errors import KafkaError, TopicAlreadyExistsError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print(" kafka-python not installed. Install with: pip install kafka-python")

class KafkaTopicManager:
    """
    Manages Kafka topics for the power-to-ammonia system
    """
    
    def __init__(self, bootstrap_servers: List[str] = None):
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.logger = logging.getLogger("kafka.topic_manager")
        
        if not KAFKA_AVAILABLE:
            self.logger.error("Kafka libraries not available")
            return
        
        # Initialize admin client
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="power2ammonia_topic_manager"
            )
            self.logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            self.admin_client = None
    
    def get_topic_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Get topic configurations for all power-to-ammonia topics
        """
        return {
            "weather-raw": {
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "segment.ms": str(24 * 60 * 60 * 1000),  # 1 day
                    "max.message.bytes": "1048576"  # 1MB
                }
            },
            "weather-processed": {
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    "cleanup.policy": "delete",
                    "compression.type": "lz4"
                }
            },
            "equipment-telemetry": {
                "partitions": 6,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "segment.ms": str(6 * 60 * 60 * 1000),  # 6 hours
                }
            },
            "equipment-status": {
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(90 * 24 * 60 * 60 * 1000),  # 90 days
                    "cleanup.policy": "compact",
                    "compression.type": "lz4"
                }
            },
            "production-metrics": {
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(365 * 24 * 60 * 60 * 1000),  # 1 year
                    "cleanup.policy": "delete",
                    "compression.type": "lz4"
                }
            },
            "energy-consumption": {
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    "cleanup.policy": "delete",
                    "compression.type": "lz4"
                }
            },
            "storage-levels": {
                "partitions": 2,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    "cleanup.policy": "delete",
                    "compression.type": "lz4"
                }
            },
            "alerts-events": {
                "partitions": 2,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(365 * 24 * 60 * 60 * 1000),  # 1 year
                    "cleanup.policy": "delete",
                    "compression.type": "snappy"
                }
            },
            "system-overview": {
                "partitions": 1,
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(90 * 24 * 60 * 60 * 1000),  # 90 days
                    "cleanup.policy": "delete",
                    "compression.type": "lz4"
                }
            }
        }
    
    def create_topics(self) -> Dict[str, bool]:
        """
        Create all topics for the power-to-ammonia system
        """
        if not self.admin_client:
            self.logger.error("Admin client not available")
            return {}
        
        topic_configs = self.get_topic_configs()
        topics_to_create = []
        results = {}
        
        # Prepare topics for creation
        for topic_name, config in topic_configs.items():
            topic = NewTopic(
                name=topic_name,
                num_partitions=config["partitions"],
                replication_factor=config["replication_factor"],
                topic_configs=config["config"]
            )
            topics_to_create.append(topic)
        
        # Create topics
        try:
            futures = self.admin_client.create_topics(topics_to_create, validate_only=False)
            
            for topic_name, future in futures.items():
                try:
                    future.result()  # Will raise exception if topic creation failed
                    results[topic_name] = True
                    self.logger.info(f" Created topic: {topic_name}")
                except TopicAlreadyExistsError:
                    results[topic_name] = True
                    self.logger.info(f" Topic already exists: {topic_name}")
                except Exception as e:
                    results[topic_name] = False
                    self.logger.error(f" Failed to create topic {topic_name}: {e}")
        
        except Exception as e:
            self.logger.error(f"Failed to create topics: {e}")
            for topic_name in topic_configs.keys():
                results[topic_name] = False
        
        return results
    
    def list_topics(self) -> List[str]:
        """
        List all topics in the cluster
        """
        if not self.admin_client:
            return []
        
        try:
            metadata = self.admin_client.list_topics()
            return list(metadata.topics.keys())
        except Exception as e:
            self.logger.error(f"Failed to list topics: {e}")
            return []
    
    def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a specific topic
        """
        if not self.admin_client:
            return False
        
        try:
            futures = self.admin_client.delete_topics([topic_name])
            futures[topic_name].result()
            self.logger.info(f"Deleted topic: {topic_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete topic {topic_name}: {e}")
            return False

class KafkaProducerWrapper:
    """
    Wrapper for Kafka producer with power-to-ammonia specific functionality
    """
    
    def __init__(self, bootstrap_servers: List[str] = None, producer_config: Dict[str, Any] = None):
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.logger = logging.getLogger("kafka.producer")
        
        if not KAFKA_AVAILABLE:
            self.logger.error("Kafka libraries not available")
            self.producer = None
            return
        
        # Default producer configuration
        default_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'retry_backoff_ms': 1000,
            'batch_size': 16384,
            'linger_ms': 10,
            'buffer_memory': 33554432,
            'compression_type': 'lz4',
            'max_in_flight_requests_per_connection': 5,
            'enable_idempotence': True
        }
        
        # Override with user config
        if producer_config:
            default_config.update(producer_config)
        
        try:
            self.producer = KafkaProducer(**default_config)
            self.logger.info(f"Kafka producer initialized for {self.bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            self.producer = None
    
    def send_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Send a message to a Kafka topic
        """
        if not self.producer:
            self.logger.error("Producer not available")
            return False
        
        try:
            # Add timestamp if not present
            if 'timestamp' not in message:
                message['timestamp'] = datetime.now().isoformat()
            
            # Send message
            future = self.producer.send(topic, value=message, key=key)
            
            # Wait for send to complete (with timeout)
            future.get(timeout=10)
            
            self.logger.debug(f"Message sent to topic {topic}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send message to {topic}: {e}")
            return False
    
    def send_batch(self, topic: str, messages: List[Dict[str, Any]], key_func: Optional[Callable] = None) -> int:
        """
        Send a batch of messages to a Kafka topic
        """
        if not self.producer:
            self.logger.error("Producer not available")
            return 0
        
        successful_sends = 0
        
        for message in messages:
            key = key_func(message) if key_func else None
            if self.send_message(topic, message, key):
                successful_sends += 1
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        self.logger.info(f"Sent {successful_sends}/{len(messages)} messages to {topic}")
        return successful_sends
    
    def close(self):
        """
        Close the producer
        """
        if self.producer:
            self.producer.close()
            self.logger.info("Kafka producer closed")

class KafkaHealthChecker:
    """
    Health checker for Kafka connectivity and topics
    """
    
    def __init__(self, bootstrap_servers: List[str] = None):
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.logger = logging.getLogger("kafka.health")
    
    def check_connectivity(self) -> bool:
        """
        Check if Kafka is reachable
        """
        if not KAFKA_AVAILABLE:
            return False
        
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=5000,
                api_version=(0, 10, 1)
            )
            producer.close()
            return True
        except Exception as e:
            self.logger.error(f"Kafka connectivity check failed: {e}")
            return False
    
    def check_topics(self, required_topics: List[str]) -> Dict[str, bool]:
        """
        Check if required topics exist
        """
        if not KAFKA_AVAILABLE:
            return {topic: False for topic in required_topics}
        
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            metadata = admin_client.list_topics()
            existing_topics = set(metadata.topics.keys())
            
            return {topic: topic in existing_topics for topic in required_topics}
        except Exception as e:
            self.logger.error(f"Topic check failed: {e}")
            return {topic: False for topic in required_topics}
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """
        Get cluster information
        """
        if not KAFKA_AVAILABLE:
            return {"error": "Kafka not available"}
        
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            metadata = admin_client.list_topics()
            
            return {
                "broker_count": len(metadata.brokers),
                "topic_count": len(metadata.topics),
                "topics": list(metadata.topics.keys()),
                "brokers": [{"id": broker.nodeId, "host": broker.host, "port": broker.port} 
                          for broker in metadata.brokers.values()]
            }
        except Exception as e:
            self.logger.error(f"Failed to get cluster info: {e}")
            return {"error": str(e)}

def setup_kafka_infrastructure(bootstrap_servers: List[str] = None) -> bool:
    """
    Complete setup of Kafka infrastructure for power-to-ammonia system
    """
    logger = logging.getLogger("kafka.setup")
    
    if not KAFKA_AVAILABLE:
        logger.error(" Kafka libraries not available. Install with: pip install kafka-python")
        return False
    
    logger.info(" Setting up Kafka infrastructure...")
    
    # Initialize components
    health_checker = KafkaHealthChecker(bootstrap_servers)
    topic_manager = KafkaTopicManager(bootstrap_servers)
    
    # Check connectivity
    logger.info("1. Checking Kafka connectivity...")
    if not health_checker.check_connectivity():
        logger.error(" Failed to connect to Kafka")
        return False
    logger.info(" Kafka connectivity confirmed")
    
    # Create topics
    logger.info("2. Creating topics...")
    topic_results = topic_manager.create_topics()
    
    success_count = sum(1 for success in topic_results.values() if success)
    total_count = len(topic_results)
    
    logger.info(f" Topic creation: {success_count}/{total_count} successful")
    
    if success_count < total_count:
        logger.warning("  Some topics failed to create")
        for topic, success in topic_results.items():
            if not success:
                logger.error(f" Failed: {topic}")
    
    # Verify topics
    logger.info("3. Verifying topics...")
    required_topics = list(topic_manager.get_topic_configs().keys())
    topic_check = health_checker.check_topics(required_topics)
    
    missing_topics = [topic for topic, exists in topic_check.items() if not exists]
    if missing_topics:
        logger.error(f" Missing topics: {missing_topics}")
        return False
    
    logger.info(" All topics verified")
    
    # Get cluster info
    cluster_info = health_checker.get_cluster_info()
    logger.info(f" Cluster info: {cluster_info.get('broker_count', 0)} brokers, {cluster_info.get('topic_count', 0)} topics")
    
    logger.info(" Kafka infrastructure setup complete!")
    return True

if __name__ == "__main__":
    # Test Kafka setup
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print(" Testing Kafka Infrastructure Setup...")
    
    # Setup infrastructure
    success = setup_kafka_infrastructure()
    
    if success:
        print(" Kafka infrastructure test passed!")
    else:
        print(" Kafka infrastructure test failed!")
        print("Make sure Kafka is running: docker-compose up kafka")