"""
Kafka producers for streaming generated data to topics.
"""

import json
import time
import threading
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
import logging
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .generator import ECommerceDataGenerator
from .config import DataGenerationConfig

logger = logging.getLogger(__name__)


class KafkaDataProducer:
    """
    Kafka producer for streaming generated e-commerce data.
    
    This producer manages streaming of different data types to their respective
    Kafka topics with configurable rates and patterns.
    """
    
    def __init__(self, 
                 generator: ECommerceDataGenerator,
                 bootstrap_servers: List[str] = None,
                 config: Dict[str, Any] = None):
        """Initialize Kafka producer."""
        self.generator = generator
        self.bootstrap_servers = bootstrap_servers or ['localhost:9092']
        
        # Kafka producer configuration
        producer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'client_id': 'ecap-data-producer',
            'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
            'key_serializer': lambda k: str(k).encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 1,
            'compression_type': 'lz4',
            'batch_size': 16384,
            'linger_ms': 10,
            'buffer_memory': 33554432
        }
        
        # Apply custom configuration
        if config:
            producer_config.update(config)
        
        self.producer = KafkaProducer(**producer_config)
        
        # Topic configuration
        self.topic_config = {
            'transactions': {
                'key_extractor': lambda x: x['user_id'],
                'rate_per_hour': 10000,
                'burst_factor': 1.5
            },
            'user-events': {
                'key_extractor': lambda x: x['session_id'],
                'rate_per_hour': 100000,
                'burst_factor': 2.0
            },
            'product-updates': {
                'key_extractor': lambda x: x['product_id'],
                'rate_per_hour': 1000,
                'burst_factor': 1.2
            },
            'fraud-alerts': {
                'key_extractor': lambda x: x['transaction_id'],
                'rate_per_hour': 100,
                'burst_factor': 3.0
            },
            'analytics-results': {
                'key_extractor': lambda x: x.get('metric_type', 'general'),
                'rate_per_hour': 500,
                'burst_factor': 1.0
            }
        }
        
        # Streaming control
        self.streaming_active = False
        self.streaming_threads = {}
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'bytes_sent': 0,
            'start_time': None,
            'topic_stats': {}
        }
    
    def start_streaming(self, 
                       topics: List[str] = None,
                       duration_hours: float = None,
                       rate_multiplier: float = 1.0) -> None:
        """Start streaming data to specified topics."""
        if self.streaming_active:
            logger.warning("Streaming already active")
            return
        
        topics = topics or list(self.topic_config.keys())
        self.streaming_active = True
        self.stats['start_time'] = datetime.now()
        
        logger.info(f"Starting streaming to topics: {topics}")
        logger.info(f"Rate multiplier: {rate_multiplier}")
        logger.info(f"Duration: {duration_hours} hours" if duration_hours else "Duration: unlimited")
        
        # Start streaming threads for each topic
        for topic in topics:
            if topic in self.topic_config:
                thread = threading.Thread(
                    target=self._stream_to_topic,
                    args=(topic, rate_multiplier, duration_hours),
                    daemon=True
                )
                thread.start()
                self.streaming_threads[topic] = thread
                logger.info(f"Started streaming thread for topic: {topic}")
            else:
                logger.warning(f"Unknown topic: {topic}")
    
    def stop_streaming(self) -> None:
        """Stop all streaming threads."""
        logger.info("Stopping streaming...")
        self.streaming_active = False
        
        # Wait for threads to finish
        for topic, thread in self.streaming_threads.items():
            thread.join(timeout=5)
            logger.info(f"Stopped streaming thread for topic: {topic}")
        
        self.streaming_threads.clear()
        logger.info("All streaming threads stopped")
    
    def _stream_to_topic(self, topic: str, rate_multiplier: float, duration_hours: float) -> None:
        """Stream data to a specific topic."""
        config = self.topic_config[topic]
        base_rate = config['rate_per_hour'] * rate_multiplier
        burst_factor = config['burst_factor']
        key_extractor = config['key_extractor']
        
        # Initialize topic stats
        self.stats['topic_stats'][topic] = {
            'messages_sent': 0,
            'messages_failed': 0,
            'bytes_sent': 0,
            'last_message_time': None
        }
        
        start_time = datetime.now()
        end_time = start_time + timedelta(hours=duration_hours) if duration_hours else None
        
        logger.info(f"Streaming to {topic} at {base_rate} messages/hour")
        
        while self.streaming_active:
            if end_time and datetime.now() > end_time:
                logger.info(f"Reached duration limit for topic: {topic}")
                break
            
            try:
                # Calculate current rate with temporal patterns
                current_rate = self._calculate_current_rate(base_rate, burst_factor)
                
                # Calculate sleep time to achieve target rate
                sleep_time = 3600 / current_rate  # seconds per message
                
                # Generate and send data
                data = self._generate_data_for_topic(topic)
                if data:
                    self._send_to_kafka(topic, data, key_extractor)
                
                # Sleep to maintain rate
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error streaming to {topic}: {e}")
                self.stats['topic_stats'][topic]['messages_failed'] += 1
                time.sleep(1)  # Brief pause on error
    
    def _calculate_current_rate(self, base_rate: float, burst_factor: float) -> float:
        """Calculate current rate based on temporal patterns."""
        current_time = datetime.now()
        
        # Get temporal context
        temporal_context = self.generator.temporal_patterns.get_temporal_context(current_time)
        
        # Apply business hour multiplier
        rate = base_rate * temporal_context.business_hour_multiplier
        
        # Apply seasonal multiplier
        rate *= temporal_context.seasonal_multiplier
        
        # Apply burst factor for peak times
        if temporal_context.hour in [10, 11, 19, 20, 21]:  # Peak hours
            rate *= burst_factor
        
        return max(rate, base_rate * 0.1)  # Minimum 10% of base rate
    
    def _generate_data_for_topic(self, topic: str) -> Optional[Dict[str, Any]]:
        """Generate data for a specific topic."""
        try:
            if topic == 'transactions':
                transactions = self.generator.generate_transactions(1)
                return transactions[0] if transactions else None
            
            elif topic == 'user-events':
                events = self.generator.generate_user_events(1)
                return events[0] if events else None
            
            elif topic == 'product-updates':
                updates = self.generator.generate_product_updates(1)
                return updates[0] if updates else None
            
            elif topic == 'fraud-alerts':
                # Generate fraud alerts based on recent transactions
                transactions = self.generator.generate_transactions(1)
                if transactions and transactions[0].get('is_fraud'):
                    return self._create_fraud_alert(transactions[0])
                return None
            
            elif topic == 'analytics-results':
                return self._generate_analytics_result()
            
            else:
                logger.warning(f"Unknown topic for data generation: {topic}")
                return None
        
        except Exception as e:
            logger.error(f"Error generating data for topic {topic}: {e}")
            return None
    
    def _create_fraud_alert(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Create fraud alert from fraudulent transaction."""
        return {
            'alert_id': f"fraud_{transaction['transaction_id']}",
            'transaction_id': transaction['transaction_id'],
            'user_id': transaction['user_id'],
            'alert_type': 'fraud_detected',
            'fraud_score': transaction['fraud_score'],
            'fraud_indicators': transaction['fraud_indicators'],
            'timestamp': datetime.now().isoformat(),
            'severity': 'high' if transaction['fraud_score'] > 0.9 else 'medium',
            'status': 'new',
            'assigned_to': None
        }
    
    def _generate_analytics_result(self) -> Dict[str, Any]:
        """Generate analytics result data."""
        metric_types = ['revenue', 'conversion_rate', 'user_activity', 'product_performance']
        metric_type = random.choice(metric_types)
        
        return {
            'result_id': f"analytics_{int(time.time())}",
            'metric_type': metric_type,
            'timestamp': datetime.now().isoformat(),
            'time_period': 'hourly',
            'value': round(random.uniform(0, 1000), 2),
            'metadata': {
                'calculation_method': 'real_time',
                'data_points': random.randint(100, 10000),
                'confidence_level': round(random.uniform(0.8, 0.99), 2)
            }
        }
    
    def _send_to_kafka(self, topic: str, data: Dict[str, Any], key_extractor: Callable) -> None:
        """Send data to Kafka topic."""
        try:
            # Extract key for partitioning
            key = key_extractor(data)
            
            # Send to Kafka
            future = self.producer.send(topic, value=data, key=key)
            
            # Handle success/failure
            record_metadata = future.get(timeout=10)
            
            # Update stats
            self.stats['messages_sent'] += 1
            self.stats['topic_stats'][topic]['messages_sent'] += 1
            self.stats['topic_stats'][topic]['last_message_time'] = datetime.now()
            
            # Estimate bytes sent
            message_size = len(json.dumps(data, default=str).encode('utf-8'))
            self.stats['bytes_sent'] += message_size
            self.stats['topic_stats'][topic]['bytes_sent'] += message_size
            
            logger.debug(f"Sent to {topic} (partition: {record_metadata.partition}, offset: {record_metadata.offset})")
            
        except KafkaError as e:
            logger.error(f"Kafka error sending to {topic}: {e}")
            self.stats['messages_failed'] += 1
            self.stats['topic_stats'][topic]['messages_failed'] += 1
            raise
        
        except Exception as e:
            logger.error(f"Error sending to {topic}: {e}")
            self.stats['messages_failed'] += 1
            self.stats['topic_stats'][topic]['messages_failed'] += 1
            raise
    
    def send_batch(self, topic: str, data: List[Dict[str, Any]], 
                   key_extractor: Callable = None) -> int:
        """Send batch of data to topic."""
        if topic not in self.topic_config:
            raise ValueError(f"Unknown topic: {topic}")
        
        if key_extractor is None:
            key_extractor = self.topic_config[topic]['key_extractor']
        
        success_count = 0
        
        logger.info(f"Sending batch of {len(data)} messages to {topic}")
        
        for item in data:
            try:
                self._send_to_kafka(topic, item, key_extractor)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to send batch item to {topic}: {e}")
        
        # Ensure all messages are sent
        self.producer.flush()
        
        logger.info(f"Sent {success_count}/{len(data)} messages to {topic}")
        return success_count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics."""
        stats = dict(self.stats)
        
        if stats['start_time']:
            elapsed = (datetime.now() - stats['start_time']).total_seconds()
            stats['elapsed_seconds'] = elapsed
            stats['messages_per_second'] = stats['messages_sent'] / elapsed if elapsed > 0 else 0
            stats['bytes_per_second'] = stats['bytes_sent'] / elapsed if elapsed > 0 else 0
        
        return stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check of producer."""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'producer_connected': False,
            'streaming_active': self.streaming_active,
            'active_threads': len(self.streaming_threads),
            'bootstrap_servers': self.bootstrap_servers,
            'stats': self.get_stats(),
            'overall_status': 'UNHEALTHY'
        }
        
        try:
            # Test connection by getting metadata
            metadata = self.producer.list_topics(timeout=5)
            health_status['producer_connected'] = True
            health_status['available_topics'] = list(metadata.topics.keys())
            
            # Check if our topics exist
            missing_topics = [topic for topic in self.topic_config.keys() 
                            if topic not in metadata.topics]
            
            if missing_topics:
                health_status['missing_topics'] = missing_topics
                health_status['overall_status'] = 'DEGRADED'
            else:
                health_status['overall_status'] = 'HEALTHY'
        
        except Exception as e:
            health_status['error'] = str(e)
            health_status['overall_status'] = 'UNHEALTHY'
        
        return health_status
    
    def close(self) -> None:
        """Close the producer and clean up resources."""
        logger.info("Closing Kafka producer...")
        
        # Stop streaming if active
        if self.streaming_active:
            self.stop_streaming()
        
        # Close producer
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


class DataGenerationOrchestrator:
    """
    Orchestrates data generation and streaming across multiple topics.
    
    This class manages the entire data generation pipeline, coordinating
    between the generator and producer to create realistic data flows.
    """
    
    def __init__(self, config: DataGenerationConfig = None):
        """Initialize orchestrator."""
        self.config = config or DataGenerationConfig()
        self.generator = ECommerceDataGenerator(self.config)
        self.producer = KafkaDataProducer(self.generator)
        
        # Scenario configurations
        self.scenarios = {
            'normal_traffic': {
                'rate_multiplier': 1.0,
                'topics': ['transactions', 'user-events', 'product-updates']
            },
            'peak_traffic': {
                'rate_multiplier': 3.0,
                'topics': ['transactions', 'user-events', 'product-updates', 'analytics-results']
            },
            'fraud_testing': {
                'rate_multiplier': 0.5,
                'topics': ['transactions', 'fraud-alerts'],
                'fraud_rate_override': 0.1  # 10% fraud rate for testing
            },
            'maintenance_mode': {
                'rate_multiplier': 0.1,
                'topics': ['user-events', 'product-updates']
            }
        }
    
    def run_scenario(self, scenario_name: str, duration_hours: float = 1.0) -> None:
        """Run a specific data generation scenario."""
        if scenario_name not in self.scenarios:
            raise ValueError(f"Unknown scenario: {scenario_name}")
        
        scenario = self.scenarios[scenario_name]
        
        logger.info(f"Starting scenario: {scenario_name}")
        logger.info(f"Duration: {duration_hours} hours")
        logger.info(f"Topics: {scenario['topics']}")
        logger.info(f"Rate multiplier: {scenario['rate_multiplier']}")
        
        # Apply scenario-specific configuration
        if 'fraud_rate_override' in scenario:
            original_fraud_rate = self.config.fraud_rate
            self.config.fraud_rate = scenario['fraud_rate_override']
            logger.info(f"Override fraud rate: {self.config.fraud_rate}")
        
        try:
            # Start streaming
            self.producer.start_streaming(
                topics=scenario['topics'],
                duration_hours=duration_hours,
                rate_multiplier=scenario['rate_multiplier']
            )
            
            # Wait for completion
            time.sleep(duration_hours * 3600)
            
        finally:
            # Stop streaming
            self.producer.stop_streaming()
            
            # Restore original configuration
            if 'fraud_rate_override' in scenario:
                self.config.fraud_rate = original_fraud_rate
        
        logger.info(f"Scenario {scenario_name} completed")
    
    def generate_historical_data(self, days: int = 30) -> Dict[str, int]:
        """Generate historical data for the specified number of days."""
        logger.info(f"Generating {days} days of historical data...")
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        # Calculate total counts based on daily rates
        daily_transactions = self.config.base_transaction_rate * 24  # per day
        daily_events = self.config.base_event_rate * 24  # per day
        daily_updates = 100  # product updates per day
        
        total_transactions = daily_transactions * days
        total_events = daily_events * days
        total_updates = daily_updates * days
        
        results = {}
        
        # Generate transactions
        logger.info(f"Generating {total_transactions} transactions...")
        transactions = self.generator.generate_transactions(
            total_transactions, start_time, end_time
        )
        results['transactions'] = self.producer.send_batch('transactions', transactions)
        
        # Generate user events
        logger.info(f"Generating {total_events} user events...")
        events = self.generator.generate_user_events(
            total_events, start_time, end_time
        )
        results['user-events'] = self.producer.send_batch('user-events', events)
        
        # Generate product updates
        logger.info(f"Generating {total_updates} product updates...")
        updates = self.generator.generate_product_updates(total_updates)
        results['product-updates'] = self.producer.send_batch('product-updates', updates)
        
        logger.info(f"Historical data generation completed: {results}")
        return results
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics from all components."""
        return {
            'generator_stats': self.generator.get_statistics(),
            'producer_stats': self.producer.get_stats(),
            'producer_health': self.producer.health_check(),
            'config': self.config.__dict__
        }
    
    def close(self) -> None:
        """Close all resources."""
        self.producer.close()
        logger.info("Data generation orchestrator closed")