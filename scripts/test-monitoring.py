#!/usr/bin/env python3
"""
Test script for monitoring and observability services in the ECAP platform.
This script verifies that all monitoring services are accessible and functioning properly.
"""

import sys
import time
import requests
import json
from typing import Dict, Any, List
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# Color codes for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

@dataclass
class ServiceConfig:
    name: str
    url: str
    port: int
    health_path: str
    expected_content: str = None
    timeout: int = 10

# Define monitoring services to test
MONITORING_SERVICES = [
    ServiceConfig(
        name="Prometheus",
        url="http://localhost",
        port=9090,
        health_path="/-/healthy",
        expected_content="Prometheus is Healthy"
    ),
    ServiceConfig(
        name="Grafana",
        url="http://localhost",
        port=3000,
        health_path="/api/health",
        expected_content="ok"
    ),
    ServiceConfig(
        name="Alertmanager",
        url="http://localhost",
        port=9093,
        health_path="/-/healthy",
        expected_content="Alertmanager is Healthy"
    ),
    ServiceConfig(
        name="Node Exporter",
        url="http://localhost",
        port=9100,
        health_path="/metrics",
        expected_content="node_"
    ),
    ServiceConfig(
        name="Postgres Exporter",
        url="http://localhost",
        port=9187,
        health_path="/metrics",
        expected_content="pg_"
    ),
    ServiceConfig(
        name="Redis Exporter",
        url="http://localhost",
        port=9121,
        health_path="/metrics",
        expected_content="redis_"
    ),
    ServiceConfig(
        name="Kafka JMX Exporter",
        url="http://localhost",
        port=5556,
        health_path="/metrics",
        expected_content="kafka_"
    )
]

def print_status(message: str, status: str = "INFO"):
    """Print colored status message."""
    color = Colors.BLUE
    if status == "SUCCESS":
        color = Colors.GREEN
    elif status == "ERROR":
        color = Colors.RED
    elif status == "WARNING":
        color = Colors.YELLOW
    
    print(f"{color}{Colors.BOLD}[{status}]{Colors.END} {message}")

def test_service(service: ServiceConfig) -> Dict[str, Any]:
    """Test a single monitoring service."""
    full_url = f"{service.url}:{service.port}{service.health_path}"
    
    try:
        print_status(f"Testing {service.name} at {full_url}", "INFO")
        
        response = requests.get(full_url, timeout=service.timeout)
        
        if response.status_code == 200:
            content = response.text
            
            # Check if expected content is present
            if service.expected_content and service.expected_content not in content:
                return {
                    'name': service.name,
                    'url': full_url,
                    'status': 'unhealthy',
                    'error': f'Expected content "{service.expected_content}" not found in response'
                }
            
            # For JSON responses, try to parse
            if service.health_path.endswith('/health'):
                try:
                    json_data = response.json()
                    if json_data.get('status') == 'ok':
                        return {
                            'name': service.name,
                            'url': full_url,
                            'status': 'healthy',
                            'response_time': response.elapsed.total_seconds(),
                            'details': json_data
                        }
                except json.JSONDecodeError:
                    pass
            
            return {
                'name': service.name,
                'url': full_url,
                'status': 'healthy',
                'response_time': response.elapsed.total_seconds(),
                'content_length': len(content)
            }
        else:
            return {
                'name': service.name,
                'url': full_url,
                'status': 'unhealthy',
                'error': f'HTTP {response.status_code}: {response.reason}'
            }
            
    except requests.exceptions.ConnectionError:
        return {
            'name': service.name,
            'url': full_url,
            'status': 'unreachable',
            'error': 'Connection refused - service may not be running'
        }
    except requests.exceptions.Timeout:
        return {
            'name': service.name,
            'url': full_url,
            'status': 'timeout',
            'error': f'Request timed out after {service.timeout} seconds'
        }
    except Exception as e:
        return {
            'name': service.name,
            'url': full_url,
            'status': 'error',
            'error': str(e)
        }

def test_prometheus_targets():
    """Test Prometheus targets status."""
    try:
        print_status("Checking Prometheus targets", "INFO")
        response = requests.get("http://localhost:9090/api/v1/targets", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            targets = data.get('data', {}).get('activeTargets', [])
            
            healthy_count = sum(1 for target in targets if target.get('health') == 'up')
            total_count = len(targets)
            
            print_status(f"Prometheus targets: {healthy_count}/{total_count} healthy", 
                        "SUCCESS" if healthy_count == total_count else "WARNING")
            
            for target in targets:
                job = target.get('labels', {}).get('job', 'unknown')
                health = target.get('health', 'unknown')
                last_scrape = target.get('lastScrape', 'unknown')
                
                status = "SUCCESS" if health == 'up' else "ERROR"
                print_status(f"  {job}: {health} (last scrape: {last_scrape})", status)
                
            return True
        else:
            print_status(f"Failed to get Prometheus targets: HTTP {response.status_code}", "ERROR")
            return False
            
    except Exception as e:
        print_status(f"Error checking Prometheus targets: {e}", "ERROR")
        return False

def test_grafana_datasources():
    """Test Grafana datasources."""
    try:
        print_status("Checking Grafana datasources", "INFO")
        
        # Basic auth for Grafana (admin:admin)
        auth = ('admin', 'admin')
        
        response = requests.get(
            "http://localhost:3000/api/datasources",
            auth=auth,
            timeout=10
        )
        
        if response.status_code == 200:
            datasources = response.json()
            
            print_status(f"Found {len(datasources)} Grafana datasources", "SUCCESS")
            
            for ds in datasources:
                name = ds.get('name', 'unknown')
                type_name = ds.get('type', 'unknown')
                url = ds.get('url', 'unknown')
                
                print_status(f"  {name} ({type_name}): {url}", "SUCCESS")
                
            return True
        else:
            print_status(f"Failed to get Grafana datasources: HTTP {response.status_code}", "ERROR")
            return False
            
    except Exception as e:
        print_status(f"Error checking Grafana datasources: {e}", "ERROR")
        return False

def main():
    """Main function to run all monitoring tests."""
    print_status("Starting monitoring services health check", "INFO")
    print("=" * 60)
    
    # Test all monitoring services concurrently
    results = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_service = {
            executor.submit(test_service, service): service 
            for service in MONITORING_SERVICES
        }
        
        for future in as_completed(future_to_service):
            result = future.result()
            results.append(result)
    
    # Print results
    print("\n" + "=" * 60)
    print_status("MONITORING SERVICES HEALTH CHECK RESULTS", "INFO")
    print("=" * 60)
    
    healthy_services = 0
    total_services = len(results)
    
    for result in results:
        name = result['name']
        status = result['status']
        
        if status == 'healthy':
            healthy_services += 1
            response_time = result.get('response_time', 0)
            print_status(f"{name:20} HEALTHY (response time: {response_time:.3f}s)", "SUCCESS")
        else:
            error = result.get('error', 'Unknown error')
            print_status(f"{name:20} {status.upper()} - {error}", "ERROR")
    
    print("\n" + "=" * 60)
    
    # Test Prometheus targets
    prometheus_ok = test_prometheus_targets()
    
    print("\n" + "=" * 60)
    
    # Test Grafana datasources
    grafana_ok = test_grafana_datasources()
    
    print("\n" + "=" * 60)
    print_status("FINAL RESULTS", "INFO")
    print("=" * 60)
    
    print_status(f"Monitoring Services: {healthy_services}/{total_services} healthy", 
                "SUCCESS" if healthy_services == total_services else "ERROR")
    
    if prometheus_ok:
        print_status("Prometheus targets: OK", "SUCCESS")
    else:
        print_status("Prometheus targets: FAILED", "ERROR")
    
    if grafana_ok:
        print_status("Grafana datasources: OK", "SUCCESS")
    else:
        print_status("Grafana datasources: FAILED", "ERROR")
    
    # Overall status
    overall_success = (healthy_services == total_services and prometheus_ok and grafana_ok)
    
    if overall_success:
        print_status("🎉 ALL MONITORING SERVICES ARE HEALTHY! 🎉", "SUCCESS")
        return 0
    else:
        print_status("❌ SOME MONITORING SERVICES HAVE ISSUES", "ERROR")
        return 1

if __name__ == "__main__":
    sys.exit(main())