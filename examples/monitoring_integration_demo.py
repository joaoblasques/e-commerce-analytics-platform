#!/usr/bin/env python3
"""
ECAP Monitoring Integration Demo.

This script demonstrates how to integrate custom metrics and tracing
into ECAP applications for comprehensive performance monitoring.

Usage:
    python examples/monitoring_integration_demo.py
"""

import asyncio
import random
import time

from fastapi import FastAPI, HTTPException

# Import ECAP monitoring components
from src.api.middleware.metrics_middleware import (
    record_analytics_job,
    record_cache_hit,
    record_cache_miss,
    record_fraud_alert,
    record_kafka_message_produced,
    record_transaction,
    record_user_registration,
)
from src.logging.structured_logger import get_logger
from src.monitoring import add_span_attributes, record_exception, start_span

logger = get_logger(__name__)

# Demo FastAPI application with monitoring
app = FastAPI(title="ECAP Monitoring Demo", version="1.0.0")


@app.get("/demo/user-registration")
async def demo_user_registration():
    """Demo endpoint showing user registration with metrics and tracing."""
    # Start custom span for tracing
    with start_span(
        "user_registration", {"operation": "demo", "service": "user-service"}
    ) as span:
        try:
            # Simulate user registration process
            user_id = f"user_{random.randint(1000, 9999)}"

            add_span_attributes(
                span,
                {"user.id": user_id, "user.type": "demo", "registration.source": "api"},
            )

            # Simulate some processing time
            processing_time = random.uniform(0.1, 0.5)
            await asyncio.sleep(processing_time)

            # Record business metric
            record_user_registration()

            # Simulate cache interaction
            if random.choice([True, False]):
                record_cache_hit("user_cache")
                logger.info("Cache hit for user data", extra={"user_id": user_id})
            else:
                record_cache_miss("user_cache")
                logger.info("Cache miss for user data", extra={"user_id": user_id})

            # Simulate Kafka message
            record_kafka_message_produced("user-events")

            logger.info(
                "User registration completed",
                extra={
                    "user_id": user_id,
                    "processing_time": processing_time,
                    "metrics_recorded": True,
                },
            )

            return {
                "status": "success",
                "user_id": user_id,
                "processing_time": processing_time,
                "metrics_recorded": [
                    "user_registration",
                    "cache_interaction",
                    "kafka_message",
                ],
            }

        except Exception as e:
            record_exception(span, e)
            logger.error(f"User registration failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Registration failed")


@app.get("/demo/transaction")
async def demo_transaction():
    """Demo endpoint showing transaction processing with comprehensive monitoring."""
    with start_span("process_transaction", {"service": "payment-service"}) as span:
        try:
            # Generate demo transaction data
            transaction_id = f"txn_{random.randint(10000, 99999)}"
            amount = round(random.uniform(10.0, 1000.0), 2)
            payment_method = random.choice(
                ["credit_card", "debit_card", "paypal", "bank_transfer"]
            )

            add_span_attributes(
                span,
                {
                    "transaction.id": transaction_id,
                    "transaction.amount": amount,
                    "payment.method": payment_method,
                },
            )

            # Simulate transaction processing
            processing_time = random.uniform(0.2, 1.0)
            await asyncio.sleep(processing_time)

            # Simulate fraud detection
            fraud_risk = random.uniform(0, 1)
            if fraud_risk > 0.95:
                # High-risk transaction
                record_fraud_alert("high_amount", "critical")
                add_span_attributes(
                    span, {"fraud.risk": "high", "fraud.score": fraud_risk}
                )

                logger.warning(
                    "High-risk transaction detected",
                    extra={
                        "transaction_id": transaction_id,
                        "fraud_score": fraud_risk,
                        "amount": amount,
                    },
                )
            elif fraud_risk > 0.8:
                # Medium-risk transaction
                record_fraud_alert("unusual_pattern", "warning")
                add_span_attributes(
                    span, {"fraud.risk": "medium", "fraud.score": fraud_risk}
                )

            # Determine transaction outcome
            success_rate = 0.95  # 95% success rate
            transaction_status = (
                "completed" if random.random() < success_rate else "failed"
            )

            # Record transaction metric
            record_transaction(transaction_status, payment_method)

            add_span_attributes(
                span,
                {
                    "transaction.status": transaction_status,
                    "transaction.fraud_score": fraud_risk,
                },
            )

            # Simulate cache operations
            record_cache_hit("transaction_cache")

            # Simulate Kafka events
            record_kafka_message_produced("transaction-events")
            record_kafka_message_produced("fraud-events")

            logger.info(
                "Transaction processed",
                extra={
                    "transaction_id": transaction_id,
                    "status": transaction_status,
                    "amount": amount,
                    "payment_method": payment_method,
                    "fraud_score": fraud_risk,
                    "processing_time": processing_time,
                },
            )

            return {
                "transaction_id": transaction_id,
                "status": transaction_status,
                "amount": amount,
                "payment_method": payment_method,
                "fraud_score": fraud_risk,
                "processing_time": processing_time,
            }

        except Exception as e:
            record_exception(span, e)
            logger.error(f"Transaction processing failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Transaction failed")


@app.get("/demo/analytics-job")
async def demo_analytics_job():
    """Demo endpoint showing analytics job execution with monitoring."""
    job_type = random.choice(
        ["customer_segmentation", "fraud_detection", "recommendation", "reporting"]
    )

    with start_span(
        "analytics_job", {"job.type": job_type, "service": "analytics-service"}
    ) as span:
        try:
            start_time = time.time()

            # Simulate analytics job processing
            job_duration = random.uniform(5, 30)  # 5-30 seconds
            await asyncio.sleep(job_duration / 10)  # Speed up for demo

            # Simulate job outcome
            success_rate = 0.9  # 90% success rate
            job_status = "completed" if random.random() < success_rate else "failed"

            actual_duration = time.time() - start_time

            add_span_attributes(
                span,
                {
                    "job.status": job_status,
                    "job.duration": actual_duration,
                    "job.records_processed": random.randint(1000, 100000),
                },
            )

            # Record analytics job metric
            record_analytics_job(job_type, job_status, actual_duration)

            # Simulate cache operations for results
            if job_status == "completed":
                record_cache_hit("analytics_cache")
            else:
                record_cache_miss("analytics_cache")

            # Simulate result publishing
            record_kafka_message_produced("analytics-results")

            logger.info(
                "Analytics job completed",
                extra={
                    "job_type": job_type,
                    "status": job_status,
                    "duration": actual_duration,
                    "records_processed": random.randint(1000, 100000),
                },
            )

            return {
                "job_type": job_type,
                "status": job_status,
                "duration": actual_duration,
                "records_processed": random.randint(1000, 100000),
            }

        except Exception as e:
            record_exception(span, e)
            record_analytics_job(job_type, "failed", time.time() - start_time)
            logger.error(f"Analytics job failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Analytics job failed")


@app.get("/demo/load-test")
async def demo_load_test(requests: int = 100):
    """Generate load for testing monitoring dashboards."""
    with start_span("load_test", {"test.requests": requests}) as span:
        try:
            results = {
                "total_requests": requests,
                "successful": 0,
                "failed": 0,
                "start_time": time.time(),
            }

            for i in range(requests):
                try:
                    # Simulate various operations
                    operation = random.choice(
                        ["registration", "transaction", "analytics"]
                    )

                    if operation == "registration":
                        record_user_registration()
                        record_cache_hit("user_cache")

                    elif operation == "transaction":
                        payment_method = random.choice(
                            ["credit_card", "debit_card", "paypal"]
                        )
                        status = "completed" if random.random() > 0.05 else "failed"
                        record_transaction(status, payment_method)

                        if random.random() > 0.95:
                            record_fraud_alert("automated_test", "low")

                    elif operation == "analytics":
                        job_type = random.choice(
                            ["segmentation", "fraud", "recommendation"]
                        )
                        status = "completed" if random.random() > 0.1 else "failed"
                        record_analytics_job(job_type, status, random.uniform(1, 10))

                    record_kafka_message_produced("load-test-events")
                    results["successful"] += 1

                    # Small delay to simulate realistic load
                    await asyncio.sleep(0.01)

                except Exception as e:
                    results["failed"] += 1
                    logger.warning(f"Load test request {i} failed: {e}")

            results["duration"] = time.time() - results["start_time"]
            results["requests_per_second"] = requests / results["duration"]

            add_span_attributes(
                span,
                {
                    "test.successful": results["successful"],
                    "test.failed": results["failed"],
                    "test.rps": results["requests_per_second"],
                },
            )

            logger.info(
                "Load test completed",
                extra={
                    "total_requests": requests,
                    "successful": results["successful"],
                    "failed": results["failed"],
                    "duration": results["duration"],
                    "rps": results["requests_per_second"],
                },
            )

            return results

        except Exception as e:
            record_exception(span, e)
            logger.error(f"Load test failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Load test failed")


@app.get("/demo/health")
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "monitoring-demo", "timestamp": time.time()}


if __name__ == "__main__":
    import uvicorn

    print("🚀 Starting ECAP Monitoring Integration Demo")
    print("📊 Available endpoints:")
    print("  GET /demo/user-registration - User registration with metrics")
    print("  GET /demo/transaction - Transaction processing with fraud detection")
    print("  GET /demo/analytics-job - Analytics job execution monitoring")
    print("  GET /demo/load-test?requests=N - Generate load for testing")
    print("  GET /demo/health - Health check")
    print("  GET /metrics - Prometheus metrics")
    print()
    print("🎯 To see metrics in action:")
    print("  1. Start monitoring stack: python monitoring/setup_monitoring.py")
    print("  2. Run this demo: python examples/monitoring_integration_demo.py")
    print(
        "  3. Generate traffic: curl http://localhost:8001/demo/load-test?requests=50"
    )
    print("  4. View dashboards: http://localhost:3001 (admin/admin123)")
    print()

    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
