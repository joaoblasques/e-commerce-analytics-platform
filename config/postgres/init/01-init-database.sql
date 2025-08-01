-- E-Commerce Analytics Platform Database Initialization
-- This script sets up the basic database structure for the platform

-- Create additional databases if needed
CREATE DATABASE ecommerce_analytics_test;
CREATE DATABASE ecommerce_analytics_dev;

-- Connect to the main database
\c ecommerce_analytics;

-- Create schemas for different domains
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS system;

-- Create a service user for the application
CREATE USER IF NOT EXISTS ecap_app_user WITH PASSWORD 'ecap_app_password';

-- Grant permissions
GRANT CONNECT ON DATABASE ecommerce_analytics TO ecap_app_user;
GRANT USAGE ON SCHEMA raw_data TO ecap_app_user;
GRANT USAGE ON SCHEMA processed_data TO ecap_app_user;
GRANT USAGE ON SCHEMA analytics TO ecap_app_user;
GRANT USAGE ON SCHEMA system TO ecap_app_user;

-- Grant table permissions (will be applied to future tables)
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ecap_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA processed_data GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ecap_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ecap_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA system GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ecap_app_user;

-- Create basic system tables
CREATE TABLE IF NOT EXISTS system.service_health (
    service_name VARCHAR(50) PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

CREATE TABLE IF NOT EXISTS system.data_pipeline_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER,
    error_message TEXT,
    metadata JSONB
);

-- Insert initial service health records
INSERT INTO system.service_health (service_name, status, details) VALUES
    ('kafka', 'initializing', '{"description": "Kafka message broker"}'),
    ('spark', 'initializing', '{"description": "Spark cluster for data processing"}'),
    ('redis', 'initializing', '{"description": "Redis cache"}'),
    ('minio', 'initializing', '{"description": "MinIO object storage"}'),
    ('postgres', 'healthy', '{"description": "PostgreSQL database"}')
ON CONFLICT (service_name) DO NOTHING;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_service_health_status ON system.service_health(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_name ON system.data_pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON system.data_pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time ON system.data_pipeline_runs(start_time);

-- Create a function to update service health
CREATE OR REPLACE FUNCTION system.update_service_health(
    p_service_name VARCHAR(50),
    p_status VARCHAR(20),
    p_details JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO system.service_health (service_name, status, last_check, details)
    VALUES (p_service_name, p_status, CURRENT_TIMESTAMP, p_details)
    ON CONFLICT (service_name)
    DO UPDATE SET
        status = EXCLUDED.status,
        last_check = EXCLUDED.last_check,
        details = EXCLUDED.details;
END;
$$ LANGUAGE plpgsql;

-- Grant execute permission on the function
GRANT EXECUTE ON FUNCTION system.update_service_health TO ecap_app_user;

-- Create a view for service health monitoring
CREATE OR REPLACE VIEW system.service_health_summary AS
SELECT
    service_name,
    status,
    last_check,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_check)) / 60 AS minutes_since_last_check,
    details
FROM system.service_health
ORDER BY service_name;

-- Grant access to the view
GRANT SELECT ON system.service_health_summary TO ecap_app_user;

-- Log successful initialization
INSERT INTO system.data_pipeline_runs (pipeline_name, status, records_processed, metadata)
VALUES ('database_initialization', 'completed', 0, '{"description": "Database initialization completed successfully"}');

-- Display completion message
SELECT 'Database initialization completed successfully!' AS status;
