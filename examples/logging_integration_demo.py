"""
ECAP Logging System Integration Demo

Demonstrates comprehensive logging features including:
- Structured JSON logging
- Request correlation tracking
- FastAPI middleware integration
- ELK stack integration
- Performance monitoring
"""

import asyncio
import time
from typing import Dict, Any
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

# Import ECAP logging components
from src.logging import (
    get_logger, 
    setup_logging, 
    LogConfig,
    LoggingMiddleware,
    PerformanceLoggingMiddleware,
    SecurityLoggingMiddleware,
    CorrelationContext,
    configure_from_settings
)
from src.config import ECAPConfig


def create_demo_app() -> FastAPI:
    """Create a demo FastAPI app with logging integration."""
    
    # Initialize configuration
    config = ECAPConfig()
    
    # Setup logging from configuration
    log_config = configure_from_settings(config.get_dict())
    setup_logging(log_config)
    
    # Create FastAPI app
    app = FastAPI(
        title="ECAP Logging Demo",
        description="Demonstration of comprehensive logging features",
        version="1.0.0"
    )
    
    # Add logging middlewares
    app.add_middleware(SecurityLoggingMiddleware)
    app.add_middleware(PerformanceLoggingMiddleware, 
                      slow_request_threshold=500.0)
    app.add_middleware(LoggingMiddleware, 
                      logger_name="ecap.demo.http",
                      exclude_paths=["/health", "/metrics"])
    
    # Get application logger
    logger = get_logger("ecap.demo.app")
    
    @app.on_event("startup")
    async def startup_event():
        """Application startup with logging."""
        logger.info("Starting ECAP Logging Demo Application", 
                   app_version="1.0.0",
                   feature_flags={"demo_mode": True})
    
    @app.on_event("shutdown")
    async def shutdown_event():
        """Application shutdown with logging."""
        logger.info("Shutting down ECAP Logging Demo Application")
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint (excluded from request logging)."""
        return {"status": "healthy", "timestamp": time.time()}
    
    @app.get("/demo/basic-logging")
    async def demo_basic_logging():
        """Demonstrate basic structured logging."""
        
        with CorrelationContext(service_name="demo-service", 
                               operation_name="basic_logging_demo") as ctx:
            
            logger.info("Starting basic logging demonstration",
                       demo_type="basic",
                       correlation_id=ctx.correlation_id)
            
            # Different log levels
            logger.debug("Debug information", debug_data={"key": "value"})
            logger.info("Information message", user_action="demo_request")
            logger.warning("Warning message", warning_type="demo_warning")
            
            # Log with structured data
            logger.info("User action performed",
                       user_id="demo-user-123",
                       action="view_logs",
                       metadata={
                           "ip_address": "192.168.1.1",
                           "user_agent": "Demo Browser",
                           "timestamp": time.time()
                       })
            
            logger.info("Basic logging demo completed",
                       duration_ms=ctx.get_duration_ms())
            
            return {
                "message": "Basic logging demonstration completed",
                "correlation_id": ctx.correlation_id,
                "examples": [
                    "debug", "info", "warning", "structured_data"
                ]
            }
    
    @app.get("/demo/database-logging")
    async def demo_database_logging():
        """Demonstrate database operation logging."""
        
        with CorrelationContext(operation_name="database_demo") as ctx:
            
            # Simulate database query
            query = "SELECT * FROM users WHERE active = true"
            start_time = time.time()
            
            # Simulate query execution time
            await asyncio.sleep(0.1)
            
            duration_ms = (time.time() - start_time) * 1000
            rows_affected = 42
            
            logger.log_database_query(
                query=query,
                duration_ms=duration_ms,
                rows_affected=rows_affected,
                table="users",
                operation="select"
            )
            
            # Simulate slow query
            slow_query = "SELECT COUNT(*) FROM large_table GROUP BY category"
            start_time = time.time()
            await asyncio.sleep(1.2)  # Simulate slow query
            slow_duration = (time.time() - start_time) * 1000
            
            logger.log_database_query(
                query=slow_query,
                duration_ms=slow_duration,
                rows_affected=1000000,
                table="large_table",
                operation="aggregate"
            )
            
            return {
                "message": "Database logging demonstration completed",
                "queries_executed": 2,
                "total_duration_ms": duration_ms + slow_duration
            }
    
    @app.get("/demo/kafka-logging")
    async def demo_kafka_logging():
        """Demonstrate Kafka event logging."""
        
        with CorrelationContext(operation_name="kafka_demo") as ctx:
            
            # Log Kafka producer events
            logger.log_kafka_event(
                topic="user-events",
                partition=0,
                offset=12345,
                event_type="producer_send",
                message_size=256,
                key="user-123"
            )
            
            logger.log_kafka_event(
                topic="user-events", 
                partition=0,
                offset=12345,
                event_type="producer_ack",
                latency_ms=15.2
            )
            
            # Log Kafka consumer events
            logger.log_kafka_event(
                topic="analytics-results",
                partition=2,
                offset=67890,
                event_type="consumer_fetch",
                batch_size=100,
                processing_time_ms=250.5
            )
            
            return {
                "message": "Kafka logging demonstration completed",
                "events_logged": 3,
                "topics": ["user-events", "analytics-results"]
            }
    
    @app.get("/demo/error-logging")
    async def demo_error_logging():
        """Demonstrate error and exception logging."""
        
        with CorrelationContext(operation_name="error_demo") as ctx:
            
            try:
                # Simulate different types of errors
                logger.warning("Potential issue detected",
                             issue_type="performance",
                             threshold_exceeded="response_time",
                             current_value=1500,
                             threshold=1000)
                
                # Simulate an exception
                raise ValueError("This is a demonstration error")
                
            except ValueError as e:
                logger.exception("Demonstration exception occurred",
                               exception_type="ValueError",
                               error_context="demo_endpoint",
                               user_impact="none",
                               recovery_action="automatic")
                
                return {
                    "message": "Error logging demonstration completed",
                    "error_handled": True,
                    "error_type": "ValueError"
                }
    
    @app.get("/demo/performance-logging")
    async def demo_performance_logging():
        """Demonstrate performance monitoring and logging."""
        
        with CorrelationContext(operation_name="performance_demo") as ctx:
            
            # Simulate various performance scenarios
            scenarios = [
                {"name": "fast_operation", "delay": 0.1},
                {"name": "acceptable_operation", "delay": 0.8},
                {"name": "slow_operation", "delay": 2.5}
            ]
            
            results = []
            
            for scenario in scenarios:
                start_time = time.time()
                await asyncio.sleep(scenario["delay"])
                duration_ms = (time.time() - start_time) * 1000
                
                # Log operation performance
                logger.info(f"Operation completed: {scenario['name']}",
                           operation_name=scenario["name"],
                           duration_ms=duration_ms,
                           performance_category="slow" if duration_ms > 1000 else "fast")
                
                results.append({
                    "operation": scenario["name"],
                    "duration_ms": round(duration_ms, 2)
                })
            
            return {
                "message": "Performance logging demonstration completed",
                "operations": results,
                "total_duration_ms": round(ctx.get_duration_ms(), 2)
            }
    
    @app.get("/demo/correlation-tracking")
    async def demo_correlation_tracking():
        """Demonstrate correlation ID tracking across operations."""
        
        with CorrelationContext(operation_name="correlation_demo") as ctx:
            
            logger.info("Starting correlation tracking demo",
                       correlation_id=ctx.correlation_id)
            
            # Simulate calling another service
            await simulate_service_call(ctx.correlation_id)
            
            # Simulate nested operations
            with ctx.create_child_span("nested_operation") as child_ctx:
                logger.info("Executing nested operation",
                           parent_correlation_id=ctx.correlation_id,
                           nested_correlation_id=child_ctx.correlation_id)
                
                await asyncio.sleep(0.2)
                
                logger.info("Nested operation completed",
                           duration_ms=child_ctx.get_duration_ms())
            
            logger.info("Correlation tracking demo completed",
                       total_duration_ms=ctx.get_duration_ms())
            
            return {
                "message": "Correlation tracking demonstration completed",
                "main_correlation_id": ctx.correlation_id,
                "trace_propagation": "demonstrated"
            }
    
    async def simulate_service_call(correlation_id: str):
        """Simulate calling another service with correlation tracking."""
        
        with CorrelationContext(correlation_id=correlation_id,
                               service_name="external-service",
                               operation_name="external_api_call") as ctx:
            
            logger.info("External service call initiated",
                       service="user-service",
                       endpoint="/api/users",
                       method="GET")
            
            await asyncio.sleep(0.3)  # Simulate network call
            
            logger.info("External service call completed",
                       status_code=200,
                       response_time_ms=ctx.get_duration_ms())
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """Global exception handler with logging."""
        logger.error("HTTP exception occurred",
                    status_code=exc.status_code,
                    detail=exc.detail,
                    path=str(request.url.path),
                    method=request.method)
        
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail}
        )
    
    return app


def main():
    """Run the logging demonstration application."""
    
    print("🚀 Starting ECAP Logging Integration Demo")
    print("📊 Available endpoints:")
    print("  - GET /demo/basic-logging")
    print("  - GET /demo/database-logging") 
    print("  - GET /demo/kafka-logging")
    print("  - GET /demo/error-logging")
    print("  - GET /demo/performance-logging")
    print("  - GET /demo/correlation-tracking")
    print()
    print("📋 Logs will be output in structured JSON format")
    print("🔍 Check Kibana at http://localhost:5601 for log visualization")
    print()
    
    # Create and run the demo app
    app = create_demo_app()
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
        access_log=False  # We handle request logging via middleware
    )


if __name__ == "__main__":
    main()