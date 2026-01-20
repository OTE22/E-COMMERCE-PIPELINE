-- PostgreSQL Initialization Script
-- Creates required schemas and roles for e-commerce analytics

-- Create extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create read-only role for BI tools
CREATE ROLE readonly_user;
GRANT CONNECT ON DATABASE ecommerce_analytics TO readonly_user;
GRANT USAGE ON SCHEMA public, staging, analytics TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public, staging, analytics TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public, staging, analytics GRANT SELECT ON TABLES TO readonly_user;

-- Create ETL role
CREATE ROLE etl_user;
GRANT CONNECT ON DATABASE ecommerce_analytics TO etl_user;
GRANT USAGE ON SCHEMA public, staging, analytics TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public, staging, analytics TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public, staging, analytics TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public, staging, analytics GRANT ALL ON TABLES TO etl_user;

-- Create API role
CREATE ROLE api_user;
GRANT CONNECT ON DATABASE ecommerce_analytics TO api_user;
GRANT USAGE ON SCHEMA public TO api_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO api_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO api_user;

-- Grant roles to main user
GRANT readonly_user, etl_user, api_user TO ecommerce;

-- Create staging tables for raw data loading
CREATE TABLE IF NOT EXISTS staging.orders_raw (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    source_file VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.customers_raw (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    source_file VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.products_raw (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    source_file VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- Create load tracking table
CREATE TABLE IF NOT EXISTS staging.load_history (
    load_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_file VARCHAR(500) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    rows_loaded INT DEFAULT 0,
    rows_failed INT DEFAULT 0,
    error_message TEXT,
    file_hash VARCHAR(64),
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_load_history_source ON staging.load_history(source_file);
CREATE INDEX idx_load_history_status ON staging.load_history(status);

-- Create date dimension for analytics
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL
);

-- Populate date dimension (5 years of dates)
INSERT INTO dim_date (
    date_key, full_date, day_of_week, day_of_month, day_of_year,
    week_of_year, month, month_name, quarter, year,
    is_weekend, fiscal_year, fiscal_quarter
)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INT as date_key,
    d as full_date,
    EXTRACT(DOW FROM d)::INT as day_of_week,
    EXTRACT(DAY FROM d)::INT as day_of_month,
    EXTRACT(DOY FROM d)::INT as day_of_year,
    EXTRACT(WEEK FROM d)::INT as week_of_year,
    EXTRACT(MONTH FROM d)::INT as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(QUARTER FROM d)::INT as quarter,
    EXTRACT(YEAR FROM d)::INT as year,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    EXTRACT(YEAR FROM d)::INT as fiscal_year,
    EXTRACT(QUARTER FROM d)::INT as fiscal_quarter
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

ANALYZE dim_date;
