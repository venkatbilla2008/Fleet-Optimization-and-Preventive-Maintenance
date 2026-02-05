-- ============================================================
-- Unity Catalog Setup for Fleet Optimization Project
-- ============================================================
-- Purpose: Create catalog, schemas, and configure permissions
-- Author: Venkat M
-- Date: 2026-01-25
-- ============================================================

-- ============================================================
-- 1. CREATE CATALOG
-- ============================================================

CREATE CATALOG IF NOT EXISTS logistics_catalog
COMMENT 'Fleet Optimization & Predictive Maintenance - Main Catalog';

USE CATALOG logistics_catalog;

DESCRIBE CATALOG EXTENDED logistics_catalog;

-- ============================================================
-- 2. CREATE SCHEMAS (Bronze, Silver, Gold)
-- ============================================================

-- Bronze Schema (Raw Data Layer)
CREATE SCHEMA IF NOT EXISTS bronze_schema
COMMENT 'Bronze Layer - Raw data from source systems (append-only, immutable)';

-- Silver Schema (Cleaned Data Layer)
CREATE SCHEMA IF NOT EXISTS silver_schema
COMMENT 'Silver Layer - Cleaned, validated, and deduplicated data';

-- Gold Schema (Business Metrics Layer)
CREATE SCHEMA IF NOT EXISTS gold_schema
COMMENT 'Gold Layer - Business KPIs, aggregates, and ML-ready features';

-- Show all schemas
SHOW SCHEMAS IN logistics_catalog;

-- ============================================================
-- 3. CREATE VOLUMES FOR DATA STORAGE
-- ============================================================

-- Volume for raw data files
CREATE VOLUME IF NOT EXISTS logistics_catalog.bronze_schema.raw_data_volume
COMMENT 'Storage for raw CSV/JSON/Parquet files before ingestion';

-- Show volumes
SHOW VOLUMES IN logistics_catalog.bronze_schema;

-- ============================================================
-- 4. ACCESS CONTROL & PERMISSIONS
-- ============================================================

-- NOTE: Uncomment and adjust the following GRANT statements
-- based on your organization's users and groups

-- ============================================================
-- 4.1 Data Engineers (Full Access)
-- ============================================================

-- GRANT ALL PRIVILEGES ON CATALOG logistics_catalog TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA logistics_catalog.bronze_schema TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA logistics_catalog.silver_schema TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA logistics_catalog.gold_schema TO `data_engineers`;

-- ============================================================
-- 4.2 Data Scientists (Read Silver/Gold, Modify Gold)
-- ============================================================

-- GRANT SELECT ON SCHEMA logistics_catalog.silver_schema TO `data_scientists`;
-- GRANT SELECT ON SCHEMA logistics_catalog.gold_schema TO `data_scientists`;
-- GRANT MODIFY ON SCHEMA logistics_catalog.gold_schema TO `data_scientists`;

-- Allow ML model creation
-- GRANT CREATE MODEL ON SCHEMA logistics_catalog.gold_schema TO `data_scientists`;

-- ============================================================
-- 4.3 Business Analysts (Read Gold Only)
-- ============================================================

-- GRANT SELECT ON SCHEMA logistics_catalog.gold_schema TO `analysts`;
-- GRANT CREATE VIEW ON SCHEMA logistics_catalog.gold_schema TO `analysts`;

-- ============================================================
-- 4.4 Executives (Dashboard Access via Views)
-- ============================================================

-- GRANT SELECT ON VIEW logistics_catalog.gold_schema.executive_dashboard TO `executives`;

-- ============================================================
-- 5. VERIFY PERMISSIONS
-- ============================================================

-- Show grants on catalog
-- SHOW GRANTS ON CATALOG logistics_catalog;

-- Show grants on schemas
-- SHOW GRANTS ON SCHEMA logistics_catalog.bronze_schema;
-- SHOW GRANTS ON SCHEMA logistics_catalog.silver_schema;
-- SHOW GRANTS ON SCHEMA logistics_catalog.gold_schema;

-- ============================================================
-- 6. DATA LINEAGE & AUDIT
-- ============================================================

-- Enable audit logging (configured at workspace level)
-- Unity Catalog automatically tracks:
-- - Table access patterns
-- - Data lineage (upstream/downstream dependencies)
-- - Schema evolution
-- - User activity

-- ============================================================
-- 7. TAGS & METADATA (Optional)
-- ============================================================

-- Add tags for data classification
-- ALTER CATALOG logistics_catalog SET TAGS ('environment' = 'production', 'project' = 'fleet_optimization');
-- ALTER SCHEMA logistics_catalog.bronze_schema SET TAGS ('layer' = 'bronze', 'data_quality' = 'raw');
-- ALTER SCHEMA logistics_catalog.silver_schema SET TAGS ('layer' = 'silver', 'data_quality' = 'cleaned');
-- ALTER SCHEMA logistics_catalog.gold_schema SET TAGS ('layer' = 'gold', 'data_quality' = 'curated');

-- ============================================================
-- 8. SUMMARY
-- ============================================================

SELECT 
    'Unity Catalog Setup Complete' AS status,
    'logistics_catalog' AS catalog_name,
    '3 schemas created (bronze, silver, gold)' AS schemas,
    '1 volume created for raw data' AS volumes,
    'Permissions configured (adjust as needed)' AS access_control;

-- ============================================================
-- END OF UNITY CATALOG SETUP
-- ============================================================
