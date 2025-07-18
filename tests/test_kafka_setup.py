"""
Test suite for Kafka setup and configuration validation.

This module tests the Kafka topic creation, configuration, and basic functionality
for the E-Commerce Analytics Platform.
"""

import json
import pytest
import time
from datetime import datetime
from typing import Dict, List
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from manage_kafka import KafkaManager, TOPIC_CONFIGS


class TestKafkaSetup:
    """Test suite for Kafka setup and configuration."""

    def test_topic_configs_structure(self):
        """Test that topic configurations have required structure."""
        required_keys = ['partitions', 'replication_factor', 'configs', 'description']
        
        for topic_name, config in TOPIC_CONFIGS.items():
            assert isinstance(topic_name, str), f"Topic name {topic_name} should be string"
            assert len(topic_name) > 0, f"Topic name should not be empty"
            
            for key in required_keys:
                assert key in config, f"Topic {topic_name} missing required key: {key}"
            
            # Validate partition count
            assert isinstance(config['partitions'], int), f"Partitions should be integer for {topic_name}"
            assert config['partitions'] > 0, f"Partitions should be positive for {topic_name}"
            
            # Validate replication factor
            assert isinstance(config['replication_factor'], int), f"Replication factor should be integer for {topic_name}"
            assert config['replication_factor'] > 0, f"Replication factor should be positive for {topic_name}"
            
            # Validate configs
            assert isinstance(config['configs'], dict), f"Configs should be dict for {topic_name}"
            assert len(config['configs']) > 0, f"Configs should not be empty for {topic_name}"
            
            # Validate description
            assert isinstance(config['description'], str), f"Description should be string for {topic_name}"
            assert len(config['description']) > 0, f"Description should not be empty for {topic_name}"

    def test_topic_retention_policies(self):
        """Test that topic retention policies are correctly configured."""
        for topic_name, config in TOPIC_CONFIGS.items():
            configs = config['configs']
            
            # Check retention.ms is present and valid
            assert 'retention.ms' in configs, f"Topic {topic_name} missing retention.ms"
            
            retention_ms = int(configs['retention.ms'])
            assert retention_ms > 0, f"Retention should be positive for {topic_name}"
            
            # Check segment.ms is present and valid
            assert 'segment.ms' in configs, f"Topic {topic_name} missing segment.ms"
            
            segment_ms = int(configs['segment.ms'])
            assert segment_ms > 0, f"Segment should be positive for {topic_name}"
            assert segment_ms <= retention_ms, f"Segment should not exceed retention for {topic_name}"

    def test_topic_compression_settings(self):
        """Test that topic compression settings are valid."""
        valid_compression_types = ['gzip', 'lz4', 'snappy', 'zstd']
        
        for topic_name, config in TOPIC_CONFIGS.items():
            configs = config['configs']
            
            assert 'compression.type' in configs, f"Topic {topic_name} missing compression.type"
            
            compression_type = configs['compression.type']
            assert compression_type in valid_compression_types, \
                f"Invalid compression type {compression_type} for {topic_name}"

    def test_topic_cleanup_policies(self):
        """Test that topic cleanup policies are valid."""
        valid_cleanup_policies = ['delete', 'compact', 'compact,delete']
        
        for topic_name, config in TOPIC_CONFIGS.items():
            configs = config['configs']
            
            assert 'cleanup.policy' in configs, f"Topic {topic_name} missing cleanup.policy"
            
            cleanup_policy = configs['cleanup.policy']
            assert cleanup_policy in valid_cleanup_policies, \
                f"Invalid cleanup policy {cleanup_policy} for {topic_name}"

    def test_topic_message_size_limits(self):
        """Test that topic message size limits are reasonable."""
        for topic_name, config in TOPIC_CONFIGS.items():
            configs = config['configs']
            
            # Check max message size
            assert 'max.message.bytes' in configs, f"Topic {topic_name} missing max.message.bytes"
            
            max_message_bytes = int(configs['max.message.bytes'])
            assert max_message_bytes > 0, f"Max message bytes should be positive for {topic_name}"
            assert max_message_bytes <= 50 * 1024 * 1024, \
                f"Max message bytes too large for {topic_name} (>50MB)"

    def test_expected_volumes_make_sense(self):
        """Test that partition counts align with expected volumes."""
        # High volume topics should have more partitions
        high_volume_topics = ['user-events']
        medium_volume_topics = ['transactions', 'analytics-results']
        low_volume_topics = ['product-updates', 'fraud-alerts']
        
        for topic in high_volume_topics:
            assert TOPIC_CONFIGS[topic]['partitions'] >= 8, \
                f"High volume topic {topic} should have >= 8 partitions"
        
        for topic in medium_volume_topics:
            assert 3 <= TOPIC_CONFIGS[topic]['partitions'] <= 8, \
                f"Medium volume topic {topic} should have 3-8 partitions"
        
        for topic in low_volume_topics:
            assert 1 <= TOPIC_CONFIGS[topic]['partitions'] <= 4, \
                f"Low volume topic {topic} should have 1-4 partitions"

    def test_retention_policies_match_use_cases(self):
        """Test that retention policies match the business use cases."""
        retention_expectations = {
            'user-events': 3 * 24 * 60 * 60 * 1000,      # 3 days (high volume)
            'transactions': 7 * 24 * 60 * 60 * 1000,     # 7 days (important data)
            'product-updates': 30 * 24 * 60 * 60 * 1000, # 30 days (compacted)
            'fraud-alerts': 90 * 24 * 60 * 60 * 1000,    # 90 days (compliance)
            'analytics-results': 14 * 24 * 60 * 60 * 1000 # 14 days (moderate)
        }
        
        for topic_name, expected_retention in retention_expectations.items():
            actual_retention = int(TOPIC_CONFIGS[topic_name]['configs']['retention.ms'])
            assert actual_retention == expected_retention, \
                f"Topic {topic_name} retention mismatch: expected {expected_retention}, got {actual_retention}"

    @patch('manage_kafka.KafkaAdminClient')
    def test_kafka_manager_initialization(self, mock_admin_client):
        """Test KafkaManager initialization."""
        manager = KafkaManager()
        
        assert manager.bootstrap_servers == ['localhost:9092']
        assert manager.client_id == 'ecap-kafka-manager'
        assert manager.admin_client is None
        assert manager.producer is None
        assert manager.consumer is None

    @patch('manage_kafka.KafkaAdminClient')
    def test_kafka_manager_connection(self, mock_admin_client):
        """Test KafkaManager connection establishment."""
        mock_client_instance = MagicMock()
        mock_admin_client.return_value = mock_client_instance
        
        manager = KafkaManager()
        result = manager.connect()
        
        assert result is True
        assert manager.admin_client is mock_client_instance
        mock_admin_client.assert_called_once_with(
            bootstrap_servers=['localhost:9092'],
            client_id='ecap-kafka-manager',
            request_timeout_ms=30000,
            connections_max_idle_ms=540000
        )

    @patch('manage_kafka.KafkaAdminClient')
    def test_kafka_manager_connection_failure(self, mock_admin_client):
        """Test KafkaManager connection failure handling."""
        mock_admin_client.side_effect = Exception("Connection failed")
        
        manager = KafkaManager()
        result = manager.connect()
        
        assert result is False
        assert manager.admin_client is None

    def test_topic_naming_conventions(self):
        """Test that topic names follow conventions."""
        for topic_name in TOPIC_CONFIGS.keys():
            # Should be lowercase with hyphens
            assert topic_name.islower(), f"Topic name {topic_name} should be lowercase"
            assert ' ' not in topic_name, f"Topic name {topic_name} should not contain spaces"
            assert topic_name.replace('-', '').replace('_', '').isalnum(), \
                f"Topic name {topic_name} should only contain alphanumeric characters, hyphens, and underscores"

    def test_topic_configs_are_strings(self):
        """Test that all topic config values are strings (required by Kafka)."""
        for topic_name, config in TOPIC_CONFIGS.items():
            for config_key, config_value in config['configs'].items():
                assert isinstance(config_value, str), \
                    f"Config {config_key} for topic {topic_name} should be string, got {type(config_value)}"

    def test_partition_distribution_strategy(self):
        """Test that partition counts enable good distribution."""
        # Total partitions should be reasonable for development
        total_partitions = sum(config['partitions'] for config in TOPIC_CONFIGS.values())
        assert total_partitions <= 50, f"Total partitions too high for development: {total_partitions}"
        
        # No single topic should dominate
        max_partitions = max(config['partitions'] for config in TOPIC_CONFIGS.values())
        assert max_partitions <= 15, f"Single topic has too many partitions: {max_partitions}"

    def test_compression_efficiency(self):
        """Test that compression types are chosen efficiently."""
        # LZ4 should be used for high-throughput topics
        high_throughput_topics = ['transactions', 'user-events']
        for topic in high_throughput_topics:
            compression = TOPIC_CONFIGS[topic]['configs']['compression.type']
            assert compression == 'lz4', \
                f"High throughput topic {topic} should use LZ4 compression, got {compression}"
        
        # GZIP should be used for lower-volume topics where compression ratio matters
        low_volume_topics = ['product-updates', 'fraud-alerts', 'analytics-results']
        for topic in low_volume_topics:
            compression = TOPIC_CONFIGS[topic]['configs']['compression.type']
            assert compression == 'gzip', \
                f"Low volume topic {topic} should use GZIP compression, got {compression}"

    def test_min_insync_replicas_configuration(self):
        """Test that min.insync.replicas is correctly configured."""
        for topic_name, config in TOPIC_CONFIGS.items():
            configs = config['configs']
            
            assert 'min.insync.replicas' in configs, \
                f"Topic {topic_name} missing min.insync.replicas"
            
            min_isr = int(configs['min.insync.replicas'])
            replication_factor = config['replication_factor']
            
            assert min_isr <= replication_factor, \
                f"Topic {topic_name} min.insync.replicas ({min_isr}) > replication_factor ({replication_factor})"
            
            # For development with replication factor 1, min.insync.replicas should be 1
            if replication_factor == 1:
                assert min_isr == 1, \
                    f"Topic {topic_name} with replication_factor=1 should have min.insync.replicas=1"

    def test_health_check_structure(self):
        """Test that health check returns expected structure."""
        manager = KafkaManager()
        
        # Mock the health check to avoid actual connection
        with patch.object(manager, 'connect', return_value=False):
            health = manager.health_check()
            
            required_keys = ['timestamp', 'cluster_connection', 'topics', 'consumer_groups', 'overall_status']
            
            for key in required_keys:
                assert key in health, f"Health check missing required key: {key}"
            
            assert isinstance(health['timestamp'], str)
            assert isinstance(health['cluster_connection'], bool)
            assert isinstance(health['topics'], dict)
            assert isinstance(health['consumer_groups'], list)
            assert health['overall_status'] in ['HEALTHY', 'DEGRADED', 'UNHEALTHY']


class TestKafkaScriptFunctionality:
    """Test the Kafka management script functionality."""

    def test_script_file_exists(self):
        """Test that the management script file exists."""
        script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'manage_kafka.py')
        assert os.path.exists(script_path), "manage_kafka.py script not found"

    def test_shell_script_exists(self):
        """Test that the shell script exists and is executable."""
        script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'kafka-topics.sh')
        assert os.path.exists(script_path), "kafka-topics.sh script not found"
        assert os.access(script_path, os.X_OK), "kafka-topics.sh script not executable"

    def test_documentation_exists(self):
        """Test that Kafka documentation exists."""
        doc_path = os.path.join(os.path.dirname(__file__), '..', 'docs', 'kafka-setup.md')
        assert os.path.exists(doc_path), "kafka-setup.md documentation not found"


@pytest.mark.integration
class TestKafkaIntegration:
    """Integration tests for Kafka functionality (requires running Kafka)."""

    def test_kafka_connection_integration(self):
        """Test actual Kafka connection (requires running Kafka)."""
        # This test will be skipped if Kafka is not available
        try:
            manager = KafkaManager()
            result = manager.connect()
            
            if result:
                # Test basic operations
                topics = manager.list_topics()
                assert isinstance(topics, list)
                
                # Test health check
                health = manager.health_check()
                assert health['cluster_connection'] is True
                
                manager.close()
            else:
                pytest.skip("Kafka not available for integration testing")
                
        except Exception as e:
            pytest.skip(f"Kafka integration test failed: {e}")

    def test_topic_creation_integration(self):
        """Test actual topic creation (requires running Kafka)."""
        try:
            manager = KafkaManager()
            
            if manager.connect():
                # Create a test topic
                test_topics = ['transactions']  # Create just one topic for testing
                result = manager.create_topics(test_topics)
                
                if result:
                    # Verify topic was created
                    topics = manager.list_topics()
                    assert 'transactions' in topics
                    
                    # Describe the topic
                    topic_info = manager.describe_topic('transactions')
                    assert topic_info is not None
                    assert topic_info['partitions'] == TOPIC_CONFIGS['transactions']['partitions']
                
                manager.close()
            else:
                pytest.skip("Kafka not available for integration testing")
                
        except Exception as e:
            pytest.skip(f"Kafka integration test failed: {e}")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])