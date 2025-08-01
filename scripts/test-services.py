#!/usr/bin/env python3
"""
Service Health Check Script
Tests connectivity to all services in the Docker Compose stack.
"""

import json
import sys
import time
from typing import Any, Dict, Optional

import psycopg2
import redis
import requests

# Service configurations
SERVICES = {
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "database": "ecommerce_analytics",
        "username": "ecap_user",
        "password": "ecap_password",
    },
    "redis": {"host": "localhost", "port": 6379, "password": "redis_password"},
    "minio": {
        "endpoint": "http://localhost:9000",
        "console": "http://localhost:9001",
        "access_key": "minioadmin",
        "secret_key": "minioadmin123",
    },
    "spark": {
        "master_ui": "http://localhost:8080",
        "worker1_ui": "http://localhost:8081",
        "worker2_ui": "http://localhost:8082",
        "history_ui": "http://localhost:18080",
    },
}


def test_postgres() -> Dict[str, Any]:
    """Test PostgreSQL connectivity."""
    try:
        conn = psycopg2.connect(
            host=SERVICES["postgres"]["host"],
            port=SERVICES["postgres"]["port"],
            database=SERVICES["postgres"]["database"],
            user=SERVICES["postgres"]["username"],
            password=SERVICES["postgres"]["password"],
        )

        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) FROM system.service_health;")
        health_count = cursor.fetchone()

        cursor.close()
        conn.close()

        return {
            "status": "healthy",
            "version": version[0] if version else "unknown",
            "health_records": health_count[0] if health_count else 0,
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def test_redis() -> Dict[str, Any]:
    """Test Redis connectivity."""
    try:
        client = redis.Redis(
            host=SERVICES["redis"]["host"],
            port=SERVICES["redis"]["port"],
            password=SERVICES["redis"]["password"],
            decode_responses=True,
        )

        # Test basic operations
        client.ping()
        client.set("test_key", "test_value")
        test_value = client.get("test_key")
        client.delete("test_key")

        info = client.info()

        return {
            "status": "healthy",
            "version": info.get("redis_version", "unknown"),
            "memory_usage": info.get("used_memory_human", "unknown"),
            "connected_clients": info.get("connected_clients", 0),
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def test_minio() -> Dict[str, Any]:
    """Test MinIO connectivity."""
    try:
        # Test health endpoint
        health_response = requests.get(
            f"{SERVICES['minio']['endpoint']}/minio/health/live", timeout=10
        )

        # Test console access
        console_response = requests.get(SERVICES["minio"]["console"], timeout=10)

        return {
            "status": "healthy",
            "health_status": health_response.status_code,
            "console_status": console_response.status_code,
            "endpoint": SERVICES["minio"]["endpoint"],
            "console": SERVICES["minio"]["console"],
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def test_spark() -> Dict[str, Any]:
    """Test Spark cluster connectivity."""
    spark_status = {}

    for component, url in SERVICES["spark"].items():
        try:
            response = requests.get(url, timeout=10)
            spark_status[component] = {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "status_code": response.status_code,
                "url": url,
            }
        except Exception as e:
            spark_status[component] = {
                "status": "unhealthy",
                "error": str(e),
                "url": url,
            }

    # Overall status
    healthy_components = sum(
        1 for comp in spark_status.values() if comp["status"] == "healthy"
    )
    total_components = len(spark_status)

    return {
        "status": "healthy" if healthy_components == total_components else "partial",
        "healthy_components": f"{healthy_components}/{total_components}",
        "components": spark_status,
    }


def test_kafka() -> Dict[str, Any]:
    """Test Kafka connectivity (basic check)."""
    try:
        # For now, just test if we can connect to the port
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(("localhost", 9092))
        sock.close()

        if result == 0:
            return {
                "status": "healthy",
                "port": 9092,
                "note": "Port connectivity verified",
            }
        else:
            return {"status": "unhealthy", "port": 9092, "error": "Port not reachable"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def print_results(results: Dict[str, Dict[str, Any]]) -> None:
    """Print formatted test results."""
    print("\n" + "=" * 60)
    print("E-COMMERCE ANALYTICS PLATFORM - SERVICE HEALTH CHECK")
    print("=" * 60)

    overall_status = "HEALTHY"

    for service_name, result in results.items():
        status = result.get("status", "unknown")
        status_icon = (
            "✅" if status == "healthy" else "⚠️" if status == "partial" else "❌"
        )

        print(f"\n{status_icon} {service_name.upper()}: {status.upper()}")

        if status != "healthy":
            overall_status = "UNHEALTHY"

        # Print service-specific details
        if service_name == "postgres":
            if "version" in result:
                print(f"   Version: {result['version'][:50]}...")
            if "health_records" in result:
                print(f"   Health Records: {result['health_records']}")

        elif service_name == "redis":
            if "version" in result:
                print(f"   Version: {result['version']}")
            if "memory_usage" in result:
                print(f"   Memory Usage: {result['memory_usage']}")
            if "connected_clients" in result:
                print(f"   Connected Clients: {result['connected_clients']}")

        elif service_name == "minio":
            if "endpoint" in result:
                print(f"   API Endpoint: {result['endpoint']}")
            if "console" in result:
                print(f"   Console: {result['console']}")

        elif service_name == "spark":
            if "healthy_components" in result:
                print(f"   Components: {result['healthy_components']}")
            if "components" in result:
                for comp, comp_result in result["components"].items():
                    comp_icon = "✅" if comp_result["status"] == "healthy" else "❌"
                    print(f"   {comp_icon} {comp}: {comp_result['url']}")

        elif service_name == "kafka":
            if "port" in result:
                print(f"   Port: {result['port']}")
            if "note" in result:
                print(f"   Note: {result['note']}")

        # Print errors if any
        if "error" in result:
            print(f"   Error: {result['error']}")

    print(f"\n{'='*60}")
    print(f"OVERALL STATUS: {overall_status}")
    print(f"{'='*60}")

    if overall_status == "HEALTHY":
        print("\n🎉 All services are healthy and ready for development!")
        print("\nNext steps:")
        print("1. Start developing your application")
        print("2. Check service UIs:")
        print("   - Spark Master: http://localhost:8080")
        print("   - MinIO Console: http://localhost:9001")
        print("   - Spark History: http://localhost:18080")
    else:
        print("\n⚠️  Some services are not healthy. Check the logs:")
        print("   docker-compose logs [service-name]")
        print("\nTo restart services:")
        print("   docker-compose restart [service-name]")


def main():
    """Main test execution."""
    print("Testing E-Commerce Analytics Platform services...")
    print("This may take a few moments...")

    results = {}

    # Test all services
    tests = [
        ("postgres", test_postgres),
        ("redis", test_redis),
        ("minio", test_minio),
        ("spark", test_spark),
        ("kafka", test_kafka),
    ]

    for service_name, test_func in tests:
        print(f"Testing {service_name}...", end=" ")
        try:
            results[service_name] = test_func()
            status = results[service_name]["status"]
            print(
                f"{'✅' if status == 'healthy' else '⚠️' if status == 'partial' else '❌'}"
            )
        except Exception as e:
            results[service_name] = {"status": "error", "error": str(e)}
            print("❌")

    # Print results
    print_results(results)

    # Exit with appropriate code
    unhealthy_services = [
        name
        for name, result in results.items()
        if result["status"] not in ["healthy", "partial"]
    ]

    if unhealthy_services:
        print(f"\nUnhealthy services: {', '.join(unhealthy_services)}")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
