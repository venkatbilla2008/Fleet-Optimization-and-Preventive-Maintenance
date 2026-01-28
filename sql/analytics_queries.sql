-- ============================================================
-- Analytics Queries for Fleet Optimization Dashboard
-- ============================================================
-- Purpose: Business intelligence queries for fleet management
-- Author: Venkat M
-- Date: 2026-01-25
-- ============================================================

USE CATALOG logistics_catalog;
USE SCHEMA gold_schema;

-- ============================================================
-- 1. FLEET HEALTH OVERVIEW
-- ============================================================

-- Current fleet health status distribution
SELECT 
    health_status,
    COUNT(*) AS vehicle_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(overall_health_score), 3) AS avg_health_score,
    ROUND(AVG(days_until_maintenance), 1) AS avg_days_until_maintenance
FROM vehicle_health_summary
GROUP BY health_status
ORDER BY 
    CASE health_status 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'WARNING' THEN 2 
        WHEN 'HEALTHY' THEN 3 
    END;

-- ============================================================
-- 2. MAINTENANCE PRIORITY LIST
-- ============================================================

-- Vehicles requiring immediate attention (next 7 days)
SELECT 
    vehicle_id,
    health_status,
    ROUND(overall_health_score, 3) AS health_score,
    days_until_maintenance,
    ROUND(engine_temp_c, 1) AS current_engine_temp,
    ROUND(oil_pressure_psi, 1) AS current_oil_pressure,
    ROUND(battery_voltage_v, 2) AS current_battery_voltage,
    ROUND(odometer_km, 0) AS odometer_km,
    engine_temp_anomaly,
    oil_pressure_anomaly,
    tire_pressure_anomaly,
    engine_anomalies_7d,
    oil_anomalies_7d
FROM vehicle_health_summary
WHERE days_until_maintenance <= 7
ORDER BY days_until_maintenance ASC, overall_health_score ASC
LIMIT 20;

-- ============================================================
-- 3. FLEET PERFORMANCE TRENDS
-- ============================================================

-- Daily fleet performance over time
SELECT 
    report_date,
    COUNT(DISTINCT vehicle_id) AS active_vehicles,
    ROUND(SUM(total_distance_km), 2) AS total_distance_km,
    ROUND(AVG(total_distance_km), 2) AS avg_distance_per_vehicle,
    ROUND(AVG(avg_speed_kmh), 2) AS fleet_avg_speed,
    ROUND(SUM(estimated_fuel_consumed_l), 2) AS total_fuel_consumed,
    ROUND(AVG(idle_time_pct), 2) AS avg_idle_time_pct,
    ROUND(AVG(avg_health_score), 3) AS fleet_avg_health_score,
    ROUND(AVG(maintenance_risk_score), 3) AS fleet_avg_risk_score,
    SUM(engine_temp_anomalies + oil_pressure_anomalies + tire_pressure_anomalies) AS total_anomalies
FROM fleet_performance_kpis
GROUP BY report_date
ORDER BY report_date DESC
LIMIT 30;

-- ============================================================
-- 4. TOP PERFORMERS & UNDERPERFORMERS
-- ============================================================

-- Top 10 most efficient vehicles (by fuel efficiency)
WITH vehicle_efficiency AS (
    SELECT 
        vehicle_id,
        SUM(total_distance_km) AS total_distance,
        SUM(estimated_fuel_consumed_l) AS total_fuel,
        ROUND(SUM(total_distance_km) / NULLIF(SUM(estimated_fuel_consumed_l), 0), 2) AS fuel_efficiency_km_per_l,
        ROUND(AVG(avg_health_score), 3) AS avg_health_score,
        SUM(engine_temp_anomalies + oil_pressure_anomalies + tire_pressure_anomalies) AS total_anomalies
    FROM fleet_performance_kpis
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY vehicle_id
    HAVING total_distance > 0 AND total_fuel > 0
)
SELECT 
    vehicle_id,
    total_distance,
    total_fuel,
    fuel_efficiency_km_per_l,
    avg_health_score,
    total_anomalies,
    'Top Performer' AS category
FROM vehicle_efficiency
ORDER BY fuel_efficiency_km_per_l DESC
LIMIT 10;

-- Bottom 10 least efficient vehicles
WITH vehicle_efficiency AS (
    SELECT 
        vehicle_id,
        SUM(total_distance_km) AS total_distance,
        SUM(estimated_fuel_consumed_l) AS total_fuel,
        ROUND(SUM(total_distance_km) / NULLIF(SUM(estimated_fuel_consumed_l), 0), 2) AS fuel_efficiency_km_per_l,
        ROUND(AVG(avg_health_score), 3) AS avg_health_score,
        SUM(engine_temp_anomalies + oil_pressure_anomalies + tire_pressure_anomalies) AS total_anomalies
    FROM fleet_performance_kpis
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY vehicle_id
    HAVING total_distance > 0 AND total_fuel > 0
)
SELECT 
    vehicle_id,
    total_distance,
    total_fuel,
    fuel_efficiency_km_per_l,
    avg_health_score,
    total_anomalies,
    'Needs Attention' AS category
FROM vehicle_efficiency
ORDER BY fuel_efficiency_km_per_l ASC
LIMIT 10;

-- ============================================================
-- 5. ANOMALY ANALYSIS
-- ============================================================

-- Vehicles with most anomalies (last 30 days)
SELECT 
    vehicle_id,
    SUM(engine_temp_anomalies) AS total_engine_anomalies,
    SUM(oil_pressure_anomalies) AS total_oil_anomalies,
    SUM(tire_pressure_anomalies) AS total_tire_anomalies,
    SUM(speed_anomalies) AS total_speed_anomalies,
    SUM(engine_temp_anomalies + oil_pressure_anomalies + 
        tire_pressure_anomalies + speed_anomalies) AS total_anomalies,
    ROUND(AVG(avg_health_score), 3) AS avg_health_score,
    ROUND(AVG(maintenance_risk_score), 3) AS avg_risk_score
FROM fleet_performance_kpis
WHERE report_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY vehicle_id
HAVING total_anomalies > 0
ORDER BY total_anomalies DESC
LIMIT 20;

-- Anomaly trends over time
SELECT 
    report_date,
    SUM(engine_temp_anomalies) AS engine_temp_anomalies,
    SUM(oil_pressure_anomalies) AS oil_pressure_anomalies,
    SUM(tire_pressure_anomalies) AS tire_pressure_anomalies,
    SUM(speed_anomalies) AS speed_anomalies,
    SUM(engine_temp_anomalies + oil_pressure_anomalies + 
        tire_pressure_anomalies + speed_anomalies) AS total_anomalies
FROM fleet_performance_kpis
GROUP BY report_date
ORDER BY report_date DESC
LIMIT 30;

-- ============================================================
-- 6. MAINTENANCE COST ANALYSIS
-- ============================================================

-- Estimated maintenance costs by vehicle
SELECT 
    vehicle_id,
    ROUND(AVG(maintenance_risk_score), 3) AS avg_risk_score,
    ROUND(AVG(avg_health_score), 3) AS avg_health_score,
    SUM(total_distance_km) AS total_distance_km,
    -- Estimated cost based on risk score
    ROUND(SUM(total_distance_km) * AVG(maintenance_risk_score) * 0.10, 2) AS estimated_maintenance_cost_usd
FROM fleet_performance_kpis
WHERE report_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY vehicle_id
ORDER BY estimated_maintenance_cost_usd DESC
LIMIT 20;

-- ============================================================
-- 7. PREDICTIVE MAINTENANCE INSIGHTS
-- ============================================================

-- Vehicles predicted to need maintenance (ML model output)
SELECT 
    vehicle_id,
    ROUND(failure_risk_score, 3) AS failure_risk_score,
    ROUND(anomaly_score, 3) AS anomaly_score,
    will_require_maintenance_7d,
    days_since_last_maintenance,
    ROUND(km_since_last_maintenance, 0) AS km_since_last_maintenance,
    ROUND(avg_engine_temp_7d, 1) AS avg_engine_temp_7d,
    ROUND(max_engine_temp_7d, 1) AS max_engine_temp_7d,
    ROUND(min_oil_pressure_7d, 1) AS min_oil_pressure_7d,
    ROUND(total_mileage_km, 0) AS total_mileage_km
FROM maintenance_prediction_features
WHERE will_require_maintenance_7d = TRUE
ORDER BY failure_risk_score DESC;

-- Maintenance prediction accuracy (if we have actual outcomes)
SELECT 
    will_require_maintenance_7d AS predicted,
    COUNT(*) AS count,
    ROUND(AVG(failure_risk_score), 3) AS avg_risk_score,
    ROUND(AVG(anomaly_score), 3) AS avg_anomaly_score
FROM maintenance_prediction_features
GROUP BY will_require_maintenance_7d;

-- ============================================================
-- 8. OPERATIONAL EFFICIENCY METRICS
-- ============================================================

-- Fleet utilization and efficiency summary
SELECT 
    COUNT(DISTINCT vehicle_id) AS total_vehicles,
    ROUND(SUM(total_distance_km), 2) AS total_distance_km,
    ROUND(AVG(total_distance_km), 2) AS avg_distance_per_vehicle,
    ROUND(SUM(estimated_fuel_consumed_l), 2) AS total_fuel_consumed_l,
    ROUND(SUM(total_distance_km) / NULLIF(SUM(estimated_fuel_consumed_l), 0), 2) AS fleet_fuel_efficiency,
    ROUND(AVG(idle_time_pct), 2) AS avg_idle_time_pct,
    ROUND(AVG(avg_speed_kmh), 2) AS avg_speed_kmh,
    ROUND(AVG(avg_health_score), 3) AS fleet_health_score,
    SUM(engine_temp_anomalies + oil_pressure_anomalies + 
        tire_pressure_anomalies + speed_anomalies) AS total_anomalies
FROM fleet_performance_kpis
WHERE report_date = (SELECT MAX(report_date) FROM fleet_performance_kpis);

-- ============================================================
-- 9. COST SAVINGS FROM PREDICTIVE MAINTENANCE
-- ============================================================

-- ROI calculation
WITH maintenance_stats AS (
    SELECT 
        COUNT(*) AS total_vehicles,
        SUM(CASE WHEN will_require_maintenance_7d = TRUE THEN 1 ELSE 0 END) AS predicted_maintenance,
        SUM(CASE WHEN will_require_maintenance_7d = FALSE THEN 1 ELSE 0 END) AS no_maintenance_needed
    FROM maintenance_prediction_features
)
SELECT 
    total_vehicles,
    predicted_maintenance,
    no_maintenance_needed,
    -- Assuming $2000 per unplanned breakdown, $500 per preventive maintenance
    predicted_maintenance * 2000 AS cost_if_reactive_usd,
    predicted_maintenance * 500 AS cost_with_predictive_usd,
    (predicted_maintenance * 2000) - (predicted_maintenance * 500) AS monthly_savings_usd,
    ((predicted_maintenance * 2000) - (predicted_maintenance * 500)) * 12 AS annual_savings_usd,
    ROUND(((predicted_maintenance * 1500.0) / (predicted_maintenance * 2000)) * 100, 2) AS savings_percentage
FROM maintenance_stats;

-- ============================================================
-- 10. EXECUTIVE DASHBOARD SUMMARY
-- ============================================================

-- High-level KPIs for executive dashboard
SELECT 
    'Fleet Overview' AS metric_category,
    COUNT(DISTINCT vehicle_id) AS total_vehicles,
    ROUND(SUM(total_distance_km), 0) AS total_distance_km,
    ROUND(AVG(avg_health_score), 3) AS fleet_health_score,
    SUM(CASE WHEN maintenance_risk_score > 0.5 THEN 1 ELSE 0 END) AS high_risk_vehicles,
    ROUND(SUM(estimated_fuel_consumed_l), 0) AS total_fuel_consumed_l,
    SUM(engine_temp_anomalies + oil_pressure_anomalies + 
        tire_pressure_anomalies + speed_anomalies) AS total_anomalies
FROM fleet_performance_kpis
WHERE report_date = (SELECT MAX(report_date) FROM fleet_performance_kpis);

-- ============================================================
-- 11. CREATE VIEWS FOR DASHBOARDS
-- ============================================================

-- View: Fleet health overview
CREATE OR REPLACE VIEW fleet_health_overview AS
SELECT 
    health_status,
    COUNT(*) AS vehicle_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(overall_health_score), 3) AS avg_health_score
FROM vehicle_health_summary
GROUP BY health_status;

-- View: Maintenance priority
CREATE OR REPLACE VIEW maintenance_priority_list AS
SELECT 
    vehicle_id,
    health_status,
    overall_health_score,
    days_until_maintenance,
    engine_temp_c,
    oil_pressure_psi,
    battery_voltage_v,
    odometer_km
FROM vehicle_health_summary
WHERE days_until_maintenance <= 14
ORDER BY days_until_maintenance ASC;

-- View: Daily fleet performance
CREATE OR REPLACE VIEW daily_fleet_performance AS
SELECT 
    report_date,
    COUNT(DISTINCT vehicle_id) AS active_vehicles,
    SUM(total_distance_km) AS total_distance_km,
    AVG(avg_speed_kmh) AS avg_speed_kmh,
    SUM(estimated_fuel_consumed_l) AS total_fuel_consumed,
    AVG(avg_health_score) AS fleet_health_score
FROM fleet_performance_kpis
GROUP BY report_date
ORDER BY report_date DESC;

-- ============================================================
-- 12. DATA QUALITY CHECKS
-- ============================================================

-- Check for data freshness
SELECT 
    'GPS Tracking' AS table_name,
    MAX(event_timestamp) AS latest_data,
    TIMESTAMPDIFF(HOUR, MAX(event_timestamp), CURRENT_TIMESTAMP()) AS hours_since_update
FROM logistics_catalog.silver_schema.gps_tracking_clean

UNION ALL

SELECT 
    'Vehicle Telemetry' AS table_name,
    MAX(event_timestamp) AS latest_data,
    TIMESTAMPDIFF(HOUR, MAX(event_timestamp), CURRENT_TIMESTAMP()) AS hours_since_update
FROM logistics_catalog.silver_schema.vehicle_telemetry_clean

UNION ALL

SELECT 
    'Fleet Performance KPIs' AS table_name,
    MAX(report_date) AS latest_data,
    DATEDIFF(CURRENT_DATE(), MAX(report_date)) AS days_since_update
FROM fleet_performance_kpis;

-- ============================================================
-- END OF ANALYTICS QUERIES
-- ============================================================

-- Summary
SELECT 
    '✓ Analytics queries ready for dashboards' AS status,
    '12 query categories defined' AS query_count,
    '3 views created for easy access' AS views_created;
