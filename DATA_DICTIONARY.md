# 📊 Data Dictionary - Fleet Optimization & Predictive Maintenance

## Overview

This document provides comprehensive schema definitions for all data tables in the Bronze, Silver, and Gold layers of the Fleet Optimization project.

---

## 🥉 Bronze Layer (Raw Data)

### 1. `bronze_schema.gps_tracking_raw`

**Description:** Raw GPS tracking data from vehicle IoT devices

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `vehicle_id` | STRING | Unique vehicle identifier | "VEH-001234" | NOT NULL |
| `timestamp` | TIMESTAMP | GPS reading timestamp (UTC) | "2026-01-25 10:30:45" | NOT NULL |
| `latitude` | DOUBLE | Latitude coordinate | 40.7128 | -90 to 90 |
| `longitude` | DOUBLE | Longitude coordinate | -74.0060 | -180 to 180 |
| `speed` | DOUBLE | Speed in km/h | 65.5 | >= 0 |
| `heading` | DOUBLE | Compass heading in degrees | 180.0 | 0 to 360 |
| `altitude` | DOUBLE | Altitude in meters | 50.2 | Optional |
| `gps_accuracy` | DOUBLE | GPS accuracy in meters | 5.0 | Optional |
| `_ingestion_time` | TIMESTAMP | Data ingestion timestamp | "2026-01-25 10:31:00" | AUTO |
| `_source_file` | STRING | Source file name | "gps_20260125.json" | AUTO |

**Partitioning:** `ingestion_date` (derived from `_ingestion_time`)  
**Volume:** ~10M records/day  
**Retention:** 2 years  

---

### 2. `bronze_schema.vehicle_telemetry_raw`

**Description:** Raw vehicle sensor data from OBD-II devices

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `vehicle_id` | STRING | Unique vehicle identifier | "VEH-001234" | NOT NULL |
| `timestamp` | TIMESTAMP | Sensor reading timestamp (UTC) | "2026-01-25 10:30:45" | NOT NULL |
| `engine_temp` | DOUBLE | Engine temperature in Celsius | 95.5 | 0 to 150 |
| `oil_pressure` | DOUBLE | Oil pressure in PSI | 45.2 | 0 to 100 |
| `tire_pressure_fl` | DOUBLE | Front-left tire pressure (PSI) | 32.0 | 20 to 50 |
| `tire_pressure_fr` | DOUBLE | Front-right tire pressure (PSI) | 32.5 | 20 to 50 |
| `tire_pressure_rl` | DOUBLE | Rear-left tire pressure (PSI) | 31.8 | 20 to 50 |
| `tire_pressure_rr` | DOUBLE | Rear-right tire pressure (PSI) | 32.2 | 20 to 50 |
| `fuel_level` | DOUBLE | Fuel level percentage | 75.0 | 0 to 100 |
| `battery_voltage` | DOUBLE | Battery voltage | 12.6 | 10 to 15 |
| `odometer` | DOUBLE | Odometer reading in km | 125678.5 | >= 0 |
| `rpm` | DOUBLE | Engine RPM | 2500.0 | 0 to 8000 |
| `throttle_position` | DOUBLE | Throttle position percentage | 45.0 | 0 to 100 |
| `brake_status` | BOOLEAN | Brake engaged (true/false) | true | - |
| `_ingestion_time` | TIMESTAMP | Data ingestion timestamp | "2026-01-25 10:31:00" | AUTO |
| `_source_file` | STRING | Source file name | "telemetry_20260125.json" | AUTO |

**Partitioning:** `ingestion_date` (derived from `_ingestion_time`)  
**Volume:** ~50M records/day  
**Retention:** 2 years  

---

### 3. `bronze_schema.delivery_records_raw`

**Description:** Raw delivery transaction records

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `delivery_id` | STRING | Unique delivery identifier | "DEL-20260125-001" | NOT NULL, PK |
| `vehicle_id` | STRING | Assigned vehicle | "VEH-001234" | NOT NULL, FK |
| `driver_id` | STRING | Assigned driver | "DRV-5678" | NOT NULL |
| `origin_address` | STRING | Pickup location | "123 Main St, NYC" | NOT NULL |
| `origin_lat` | DOUBLE | Origin latitude | 40.7128 | -90 to 90 |
| `origin_lon` | DOUBLE | Origin longitude | -74.0060 | -180 to 180 |
| `destination_address` | STRING | Delivery location | "456 Oak Ave, Brooklyn" | NOT NULL |
| `destination_lat` | DOUBLE | Destination latitude | 40.6782 | -90 to 90 |
| `destination_lon` | DOUBLE | Destination longitude | -73.9442 | -180 to 180 |
| `scheduled_pickup_time` | TIMESTAMP | Scheduled pickup | "2026-01-25 09:00:00" | NOT NULL |
| `actual_pickup_time` | TIMESTAMP | Actual pickup | "2026-01-25 09:15:00" | Optional |
| `scheduled_delivery_time` | TIMESTAMP | Scheduled delivery | "2026-01-25 11:00:00" | NOT NULL |
| `actual_delivery_time` | TIMESTAMP | Actual delivery | "2026-01-25 11:30:00" | Optional |
| `status` | STRING | Delivery status | "COMPLETED" | ENUM |
| `package_count` | INT | Number of packages | 5 | >= 1 |
| `total_weight_kg` | DOUBLE | Total weight | 25.5 | >= 0 |
| `delivery_notes` | STRING | Special instructions | "Ring doorbell" | Optional |
| `_ingestion_time` | TIMESTAMP | Data ingestion timestamp | "2026-01-25 12:00:00" | AUTO |
| `_source_file` | STRING | Source file name | "deliveries_20260125.csv" | AUTO |

**Status ENUM:** `SCHEDULED`, `IN_PROGRESS`, `COMPLETED`, `FAILED`, `CANCELLED`  
**Partitioning:** `scheduled_delivery_date` (derived from `scheduled_delivery_time`)  
**Volume:** ~500K records/day  
**Retention:** 5 years (compliance)  

---

### 4. `bronze_schema.maintenance_logs_raw`

**Description:** Raw vehicle maintenance records

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `maintenance_id` | STRING | Unique maintenance identifier | "MNT-20260115-001" | NOT NULL, PK |
| `vehicle_id` | STRING | Vehicle serviced | "VEH-001234" | NOT NULL, FK |
| `maintenance_date` | DATE | Date of service | "2026-01-15" | NOT NULL |
| `maintenance_type` | STRING | Type of maintenance | "PREVENTIVE" | ENUM |
| `service_category` | STRING | Service category | "OIL_CHANGE" | ENUM |
| `parts_replaced` | STRING | Parts replaced (comma-separated) | "Oil filter, Air filter" | Optional |
| `labor_hours` | DOUBLE | Labor hours | 2.5 | >= 0 |
| `parts_cost` | DOUBLE | Parts cost in USD | 150.00 | >= 0 |
| `labor_cost` | DOUBLE | Labor cost in USD | 200.00 | >= 0 |
| `total_cost` | DOUBLE | Total cost in USD | 350.00 | >= 0 |
| `downtime_hours` | DOUBLE | Vehicle downtime | 4.0 | >= 0 |
| `mechanic_id` | STRING | Mechanic who performed service | "MECH-123" | Optional |
| `mechanic_notes` | STRING | Free-text notes | "Replaced worn brake pads" | Optional |
| `odometer_reading` | DOUBLE | Odometer at service time | 125000.0 | >= 0 |
| `next_service_due_km` | DOUBLE | Next service due at (km) | 130000.0 | Optional |
| `_ingestion_time` | TIMESTAMP | Data ingestion timestamp | "2026-01-15 18:00:00" | AUTO |
| `_source_file` | STRING | Source file name | "maintenance_jan2026.csv" | AUTO |

**Maintenance Type ENUM:** `PREVENTIVE`, `CORRECTIVE`, `EMERGENCY`  
**Service Category ENUM:** `OIL_CHANGE`, `TIRE_ROTATION`, `BRAKE_SERVICE`, `ENGINE_REPAIR`, `TRANSMISSION`, `ELECTRICAL`, `INSPECTION`, `OTHER`  
**Partitioning:** `maintenance_date`  
**Volume:** ~10K records/month  
**Retention:** 10 years (compliance)  

---

### 5. `bronze_schema.weather_data_raw`

**Description:** Raw weather data from external API

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `location_id` | STRING | Location identifier | "NYC-MANHATTAN" | NOT NULL |
| `timestamp` | TIMESTAMP | Weather observation time | "2026-01-25 10:00:00" | NOT NULL |
| `latitude` | DOUBLE | Location latitude | 40.7128 | -90 to 90 |
| `longitude` | DOUBLE | Location longitude | -74.0060 | -180 to 180 |
| `temperature` | DOUBLE | Temperature in Celsius | 5.5 | -50 to 50 |
| `feels_like` | DOUBLE | Feels-like temperature | 2.0 | -50 to 50 |
| `humidity` | DOUBLE | Humidity percentage | 65.0 | 0 to 100 |
| `precipitation` | DOUBLE | Precipitation in mm | 2.5 | >= 0 |
| `wind_speed` | DOUBLE | Wind speed in km/h | 20.0 | >= 0 |
| `wind_direction` | DOUBLE | Wind direction in degrees | 180.0 | 0 to 360 |
| `visibility` | DOUBLE | Visibility in km | 10.0 | >= 0 |
| `weather_condition` | STRING | Weather description | "Light rain" | - |
| `road_condition` | STRING | Road condition | "WET" | ENUM |
| `_ingestion_time` | TIMESTAMP | Data ingestion timestamp | "2026-01-25 10:05:00" | AUTO |
| `_source_file` | STRING | Source API endpoint | "weather_api_v2" | AUTO |

**Road Condition ENUM:** `DRY`, `WET`, `ICY`, `SNOWY`, `FLOODED`  
**Partitioning:** `observation_date` (derived from `timestamp`)  
**Volume:** ~1K records/hour  
**Retention:** 1 year  

---

## 🥈 Silver Layer (Cleaned & Validated)

### 1. `silver_schema.gps_tracking_clean`

**Description:** Cleaned and validated GPS tracking data

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `vehicle_id` | STRING | Unique vehicle identifier | "VEH-001234" | NOT NULL |
| `event_timestamp` | TIMESTAMP | GPS reading timestamp (UTC) | "2026-01-25 10:30:45" | NOT NULL |
| `latitude` | DOUBLE | Validated latitude | 40.7128 | -90 to 90 |
| `longitude` | DOUBLE | Validated longitude | -74.0060 | -180 to 180 |
| `speed_kmh` | DOUBLE | Speed in km/h | 65.5 | >= 0 |
| `heading_degrees` | DOUBLE | Compass heading | 180.0 | 0 to 360 |
| `altitude_m` | DOUBLE | Altitude in meters | 50.2 | Optional |
| `gps_accuracy_m` | DOUBLE | GPS accuracy | 5.0 | Optional |
| `is_valid_location` | BOOLEAN | Location within operational area | true | - |
| `location_region` | STRING | Geographic region | "NYC-MANHATTAN" | - |
| `distance_from_prev_km` | DOUBLE | Distance from previous GPS point | 0.5 | >= 0 |
| `time_from_prev_sec` | DOUBLE | Time from previous GPS point | 30.0 | >= 0 |
| `calculated_speed_kmh` | DOUBLE | Speed calculated from GPS delta | 60.0 | >= 0 |
| `speed_anomaly_flag` | BOOLEAN | Speed anomaly detected | false | - |
| `_processed_time` | TIMESTAMP | Processing timestamp | "2026-01-25 10:35:00" | AUTO |
| `_data_quality_score` | DOUBLE | Quality score (0-1) | 0.98 | 0 to 1 |

**Business Rules Applied:**
- Remove GPS points outside operational regions (lat/lon validation)
- Flag speed anomalies (> 120 km/h or sudden changes)
- Calculate distance and speed from GPS deltas
- Deduplicate by `vehicle_id` + `event_timestamp`

**Partitioning:** `event_date`, `vehicle_id`  
**Z-Ordering:** `vehicle_id`, `event_timestamp`  
**Volume:** ~9.5M records/day (5% filtered out)  

---

### 2. `silver_schema.vehicle_telemetry_clean`

**Description:** Cleaned and validated vehicle sensor data

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `vehicle_id` | STRING | Unique vehicle identifier | "VEH-001234" | NOT NULL |
| `event_timestamp` | TIMESTAMP | Sensor reading timestamp | "2026-01-25 10:30:45" | NOT NULL |
| `engine_temp_c` | DOUBLE | Engine temperature (validated) | 95.5 | 0 to 150 |
| `engine_temp_anomaly` | BOOLEAN | Temperature anomaly flag | false | - |
| `oil_pressure_psi` | DOUBLE | Oil pressure (validated) | 45.2 | 0 to 100 |
| `oil_pressure_anomaly` | BOOLEAN | Pressure anomaly flag | false | - |
| `avg_tire_pressure_psi` | DOUBLE | Average tire pressure | 32.1 | 20 to 50 |
| `tire_pressure_variance` | DOUBLE | Variance across 4 tires | 0.3 | >= 0 |
| `tire_pressure_anomaly` | BOOLEAN | Tire pressure issue flag | false | - |
| `fuel_level_pct` | DOUBLE | Fuel level percentage | 75.0 | 0 to 100 |
| `battery_voltage_v` | DOUBLE | Battery voltage | 12.6 | 10 to 15 |
| `battery_health_flag` | BOOLEAN | Battery health OK | true | - |
| `odometer_km` | DOUBLE | Odometer reading | 125678.5 | >= 0 |
| `rpm` | DOUBLE | Engine RPM | 2500.0 | 0 to 8000 |
| `throttle_pct` | DOUBLE | Throttle position | 45.0 | 0 to 100 |
| `is_braking` | BOOLEAN | Brake engaged | true | - |
| `is_idling` | BOOLEAN | Engine idling (RPM < 1000, speed = 0) | false | - |
| `overall_health_score` | DOUBLE | Composite health score (0-1) | 0.92 | 0 to 1 |
| `_processed_time` | TIMESTAMP | Processing timestamp | "2026-01-25 10:35:00" | AUTO |
| `_data_quality_score` | DOUBLE | Quality score (0-1) | 0.95 | 0 to 1 |

**Business Rules Applied:**
- Validate sensor ranges (engine temp, oil pressure, etc.)
- Flag anomalies using statistical thresholds (mean ± 3σ)
- Calculate derived metrics (avg tire pressure, variance)
- Detect idling (RPM < 1000 and speed = 0)
- Compute overall health score

**Partitioning:** `event_date`, `vehicle_id`  
**Z-Ordering:** `vehicle_id`, `event_timestamp`  
**Volume:** ~48M records/day (4% filtered out)  

---

### 3. `silver_schema.delivery_records_clean`

**Description:** Cleaned and validated delivery records

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `delivery_id` | STRING | Unique delivery identifier | "DEL-20260125-001" | NOT NULL, PK |
| `vehicle_id` | STRING | Assigned vehicle | "VEH-001234" | NOT NULL, FK |
| `driver_id` | STRING | Assigned driver | "DRV-5678" | NOT NULL |
| `origin_address` | STRING | Pickup location | "123 Main St, NYC" | NOT NULL |
| `origin_lat` | DOUBLE | Origin latitude | 40.7128 | -90 to 90 |
| `origin_lon` | DOUBLE | Origin longitude | -74.0060 | -180 to 180 |
| `destination_address` | STRING | Delivery location | "456 Oak Ave, Brooklyn" | NOT NULL |
| `destination_lat` | DOUBLE | Destination latitude | 40.6782 | -90 to 90 |
| `destination_lon` | DOUBLE | Destination longitude | -73.9442 | -180 to 180 |
| `scheduled_pickup_time` | TIMESTAMP | Scheduled pickup | "2026-01-25 09:00:00" | NOT NULL |
| `actual_pickup_time` | TIMESTAMP | Actual pickup | "2026-01-25 09:15:00" | Optional |
| `scheduled_delivery_time` | TIMESTAMP | Scheduled delivery | "2026-01-25 11:00:00" | NOT NULL |
| `actual_delivery_time` | TIMESTAMP | Actual delivery | "2026-01-25 11:30:00" | Optional |
| `status` | STRING | Delivery status | "COMPLETED" | ENUM |
| `package_count` | INT | Number of packages | 5 | >= 1 |
| `total_weight_kg` | DOUBLE | Total weight | 25.5 | >= 0 |
| `delivery_notes` | STRING | Special instructions | "Ring doorbell" | Optional |
| `distance_km` | DOUBLE | Calculated distance | 12.5 | >= 0 |
| `estimated_duration_min` | DOUBLE | Estimated duration | 45.0 | >= 0 |
| `actual_duration_min` | DOUBLE | Actual duration | 55.0 | >= 0 |
| `pickup_delay_min` | DOUBLE | Pickup delay | 15.0 | - |
| `delivery_delay_min` | DOUBLE | Delivery delay | 30.0 | - |
| `is_on_time` | BOOLEAN | Delivered on time | false | - |
| `route_efficiency_score` | DOUBLE | Efficiency score (0-1) | 0.82 | 0 to 1 |
| `_processed_time` | TIMESTAMP | Processing timestamp | "2026-01-25 12:05:00" | AUTO |
| `_data_quality_score` | DOUBLE | Quality score (0-1) | 1.0 | 0 to 1 |

**Business Rules Applied:**
- Calculate distance using Haversine formula
- Compute delays (actual - scheduled)
- Flag on-time deliveries (delay <= 15 minutes)
- Calculate route efficiency score
- Validate timestamp logic (actual >= scheduled)

**Partitioning:** `scheduled_delivery_date`  
**Z-Ordering:** `vehicle_id`, `driver_id`, `scheduled_delivery_time`  
**Volume:** ~495K records/day (1% filtered out)  

---

### 4. `silver_schema.maintenance_logs_clean`

**Description:** Cleaned and validated maintenance records

| Column Name | Data Type | Description | Example | Constraints |
|-------------|-----------|-------------|---------|-------------|
| `maintenance_id` | STRING | Unique maintenance identifier | "MNT-20260115-001" | NOT NULL, PK |
| `vehicle_id` | STRING | Vehicle serviced | "VEH-001234" | NOT NULL, FK |
| `maintenance_date` | DATE | Date of service | "2026-01-15" | NOT NULL |
| `maintenance_type` | STRING | Type of maintenance | "PREVENTIVE" | ENUM |
| `service_category` | STRING | Service category | "OIL_CHANGE" | ENUM |
| `parts_replaced_list` | ARRAY<STRING> | Parts replaced (array) | ["Oil filter", "Air filter"] | Optional |
| `labor_hours` | DOUBLE | Labor hours | 2.5 | >= 0 |
| `parts_cost_usd` | DOUBLE | Parts cost | 150.00 | >= 0 |
| `labor_cost_usd` | DOUBLE | Labor cost | 200.00 | >= 0 |
| `total_cost_usd` | DOUBLE | Total cost | 350.00 | >= 0 |
| `downtime_hours` | DOUBLE | Vehicle downtime | 4.0 | >= 0 |
| `mechanic_id` | STRING | Mechanic identifier | "MECH-123" | Optional |
| `mechanic_notes_clean` | STRING | Cleaned notes | "Replaced worn brake pads" | Optional |
| `odometer_reading_km` | DOUBLE | Odometer at service | 125000.0 | >= 0 |
| `km_since_last_service` | DOUBLE | Distance since last service | 5000.0 | >= 0 |
| `days_since_last_service` | INT | Days since last service | 90 | >= 0 |
| `next_service_due_km` | DOUBLE | Next service due at | 130000.0 | Optional |
| `is_emergency` | BOOLEAN | Emergency maintenance | false | - |
| `cost_per_km` | DOUBLE | Cost per km driven | 0.07 | >= 0 |
| `_processed_time` | TIMESTAMP | Processing timestamp | "2026-01-15 18:05:00" | AUTO |
| `_data_quality_score` | DOUBLE | Quality score (0-1) | 0.98 | 0 to 1 |

**Business Rules Applied:**
- Parse comma-separated parts into array
- Calculate km/days since last service
- Compute cost per km
- Flag emergency maintenance
- Clean mechanic notes (remove special characters)

**Partitioning:** `maintenance_date`  
**Z-Ordering:** `vehicle_id`, `maintenance_date`  
**Volume:** ~9.8K records/month (2% filtered out)  

---

## 🥇 Gold Layer (Business Metrics & Features)

### 1. `gold_schema.fleet_performance_kpis`

**Description:** Daily fleet performance metrics

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| `report_date` | DATE | Reporting date | "2026-01-25" |
| `vehicle_id` | STRING | Vehicle identifier | "VEH-001234" |
| `total_distance_km` | DOUBLE | Total distance driven | 250.5 |
| `total_deliveries` | INT | Number of deliveries | 15 |
| `successful_deliveries` | INT | Successful deliveries | 14 |
| `failed_deliveries` | INT | Failed deliveries | 1 |
| `on_time_deliveries` | INT | On-time deliveries | 12 |
| `on_time_rate_pct` | DOUBLE | On-time delivery rate | 80.0 |
| `avg_delivery_duration_min` | DOUBLE | Average delivery time | 45.5 |
| `total_fuel_consumed_l` | DOUBLE | Fuel consumed (estimated) | 25.0 |
| `fuel_efficiency_km_per_l` | DOUBLE | Fuel efficiency | 10.0 |
| `total_idle_time_min` | DOUBLE | Total idle time | 120.0 |
| `idle_time_pct` | DOUBLE | Idle time percentage | 15.0 |
| `avg_speed_kmh` | DOUBLE | Average speed | 55.0 |
| `max_speed_kmh` | DOUBLE | Maximum speed | 95.0 |
| `hard_braking_events` | INT | Hard braking count | 5 |
| `rapid_acceleration_events` | INT | Rapid acceleration count | 3 |
| `avg_engine_temp_c` | DOUBLE | Average engine temp | 92.0 |
| `max_engine_temp_c` | DOUBLE | Maximum engine temp | 105.0 |
| `avg_oil_pressure_psi` | DOUBLE | Average oil pressure | 45.0 |
| `min_oil_pressure_psi` | DOUBLE | Minimum oil pressure | 38.0 |
| `overall_health_score` | DOUBLE | Vehicle health score (0-1) | 0.92 |
| `maintenance_risk_score` | DOUBLE | Maintenance risk (0-1) | 0.15 |

**Partitioning:** `report_date`  
**Z-Ordering:** `vehicle_id`, `report_date`  
**Update Frequency:** Daily  

---

### 2. `gold_schema.delivery_efficiency_metrics`

**Description:** Route and delivery efficiency metrics

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| `report_date` | DATE | Reporting date | "2026-01-25" |
| `route_id` | STRING | Route identifier | "ROUTE-NYC-001" |
| `vehicle_id` | STRING | Vehicle identifier | "VEH-001234" |
| `driver_id` | STRING | Driver identifier | "DRV-5678" |
| `total_deliveries` | INT | Deliveries on route | 20 |
| `total_distance_km` | DOUBLE | Total distance | 150.0 |
| `total_duration_min` | DOUBLE | Total duration | 480.0 |
| `avg_distance_per_delivery_km` | DOUBLE | Avg distance per delivery | 7.5 |
| `avg_duration_per_delivery_min` | DOUBLE | Avg duration per delivery | 24.0 |
| `route_efficiency_score` | DOUBLE | Efficiency score (0-1) | 0.85 |
| `fuel_cost_usd` | DOUBLE | Estimated fuel cost | 45.00 |
| `cost_per_delivery_usd` | DOUBLE | Cost per delivery | 2.25 |
| `on_time_rate_pct` | DOUBLE | On-time delivery rate | 85.0 |
| `avg_delay_min` | DOUBLE | Average delay | 12.0 |
| `weather_impact_score` | DOUBLE | Weather impact (0-1) | 0.3 |

**Partitioning:** `report_date`  
**Z-Ordering:** `route_id`, `vehicle_id`, `driver_id`  
**Update Frequency:** Daily  

---

### 3. `gold_schema.maintenance_prediction_features`

**Description:** ML-ready features for predictive maintenance

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| `feature_date` | DATE | Feature snapshot date | "2026-01-25" |
| `vehicle_id` | STRING | Vehicle identifier | "VEH-001234" |
| `vehicle_age_years` | DOUBLE | Vehicle age | 3.5 |
| `vehicle_model` | STRING | Vehicle model | "Ford Transit" |
| `total_mileage_km` | DOUBLE | Total mileage | 125678.5 |
| `avg_daily_mileage_30d` | DOUBLE | Avg daily mileage (30d) | 150.0 |
| `days_since_last_maintenance` | INT | Days since last service | 45 |
| `km_since_last_maintenance` | DOUBLE | Km since last service | 6750.0 |
| `maintenance_frequency_30d` | INT | Maintenance events (30d) | 0 |
| `maintenance_frequency_90d` | INT | Maintenance events (90d) | 2 |
| `avg_engine_temp_7d` | DOUBLE | Avg engine temp (7d) | 93.5 |
| `max_engine_temp_7d` | DOUBLE | Max engine temp (7d) | 108.0 |
| `std_engine_temp_7d` | DOUBLE | Std dev engine temp (7d) | 5.2 |
| `avg_oil_pressure_7d` | DOUBLE | Avg oil pressure (7d) | 44.5 |
| `min_oil_pressure_7d` | DOUBLE | Min oil pressure (7d) | 36.0 |
| `tire_pressure_variance_7d` | DOUBLE | Tire pressure variance (7d) | 1.2 |
| `battery_voltage_trend` | DOUBLE | Battery voltage trend | -0.05 |
| `hard_braking_events_7d` | INT | Hard braking (7d) | 15 |
| `rapid_acceleration_events_7d` | INT | Rapid acceleration (7d) | 8 |
| `idle_time_pct_7d` | DOUBLE | Idle time % (7d) | 18.0 |
| `rain_exposure_days_30d` | INT | Rain days (30d) | 8 |
| `snow_exposure_days_30d` | INT | Snow days (30d) | 2 |
| `extreme_temp_hours_30d` | DOUBLE | Extreme temp hours (30d) | 50.0 |
| `cumulative_maintenance_cost_usd` | DOUBLE | Total maintenance cost | 5000.00 |
| `cost_per_km_usd` | DOUBLE | Cost per km | 0.04 |
| `anomaly_score` | DOUBLE | Anomaly score (0-1) | 0.12 |
| `failure_risk_score` | DOUBLE | Failure risk (0-1) | 0.25 |
| `will_require_maintenance_7d` | BOOLEAN | Target variable (label) | false |

**Partitioning:** `feature_date`  
**Z-Ordering:** `vehicle_id`, `feature_date`  
**Update Frequency:** Daily  
**Purpose:** ML model training and inference  

---

### 4. `gold_schema.cost_optimization_aggregates`

**Description:** Financial and cost optimization metrics

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| `report_month` | DATE | Reporting month (first day) | "2026-01-01" |
| `vehicle_id` | STRING | Vehicle identifier | "VEH-001234" |
| `total_distance_km` | DOUBLE | Total distance | 4500.0 |
| `total_deliveries` | INT | Total deliveries | 300 |
| `fuel_consumed_l` | DOUBLE | Fuel consumed | 450.0 |
| `fuel_cost_usd` | DOUBLE | Fuel cost | 675.00 |
| `maintenance_cost_usd` | DOUBLE | Maintenance cost | 500.00 |
| `labor_cost_usd` | DOUBLE | Labor cost (driver) | 3000.00 |
| `total_operational_cost_usd` | DOUBLE | Total cost | 4175.00 |
| `cost_per_km_usd` | DOUBLE | Cost per km | 0.93 |
| `cost_per_delivery_usd` | DOUBLE | Cost per delivery | 13.92 |
| `revenue_per_delivery_usd` | DOUBLE | Revenue per delivery | 20.00 |
| `profit_margin_pct` | DOUBLE | Profit margin | 30.4 |
| `downtime_hours` | DOUBLE | Total downtime | 12.0 |
| `downtime_cost_usd` | DOUBLE | Downtime cost | 600.00 |
| `preventive_maintenance_cost_usd` | DOUBLE | Preventive cost | 300.00 |
| `corrective_maintenance_cost_usd` | DOUBLE | Corrective cost | 200.00 |
| `cost_savings_from_prediction_usd` | DOUBLE | Savings from predictive maintenance | 150.00 |

**Partitioning:** `report_month`  
**Z-Ordering:** `vehicle_id`, `report_month`  
**Update Frequency:** Monthly  

---

## 🔑 Primary Keys & Foreign Keys

### Relationships:

```
bronze_schema.vehicle_telemetry_raw.vehicle_id → vehicle_master.vehicle_id
bronze_schema.gps_tracking_raw.vehicle_id → vehicle_master.vehicle_id
bronze_schema.delivery_records_raw.vehicle_id → vehicle_master.vehicle_id
bronze_schema.delivery_records_raw.driver_id → driver_master.driver_id
bronze_schema.maintenance_logs_raw.vehicle_id → vehicle_master.vehicle_id
```

---

## 📏 Data Quality Rules

### Bronze Layer:
- **Completeness:** Required fields must not be null
- **Validity:** Values must be within defined ranges
- **Timeliness:** Ingestion within 5 minutes of source timestamp

### Silver Layer:
- **Accuracy:** GPS coordinates within operational regions
- **Consistency:** Timestamps follow logical order
- **Uniqueness:** No duplicates on business keys
- **Integrity:** Foreign keys must exist in master tables

### Gold Layer:
- **Aggregation Accuracy:** Metrics match source data
- **Freshness:** Updated within SLA (daily/monthly)
- **Completeness:** All vehicles/routes represented

---

## 📊 Data Volume Estimates

| Layer | Table | Daily Volume | Monthly Volume | Annual Volume |
|-------|-------|--------------|----------------|---------------|
| Bronze | gps_tracking_raw | 10M | 300M | 3.6B |
| Bronze | vehicle_telemetry_raw | 50M | 1.5B | 18B |
| Bronze | delivery_records_raw | 500K | 15M | 180M |
| Bronze | maintenance_logs_raw | 333 | 10K | 120K |
| Bronze | weather_data_raw | 24K | 720K | 8.6M |
| Silver | gps_tracking_clean | 9.5M | 285M | 3.4B |
| Silver | vehicle_telemetry_clean | 48M | 1.44B | 17.3B |
| Silver | delivery_records_clean | 495K | 14.85M | 178M |
| Silver | maintenance_logs_clean | 327 | 9.8K | 118K |
| Gold | fleet_performance_kpis | 500 | 15K | 180K |
| Gold | delivery_efficiency_metrics | 100 | 3K | 36K |
| Gold | maintenance_prediction_features | 500 | 15K | 180K |
| Gold | cost_optimization_aggregates | - | 500 | 6K |

---

## 🔐 Data Sensitivity & Compliance

| Data Element | Sensitivity Level | Compliance Requirement |
|--------------|-------------------|------------------------|
| GPS coordinates | Medium | GDPR (location data) |
| Driver information | High | GDPR, CCPA (PII) |
| Delivery addresses | High | GDPR, CCPA (PII) |
| Vehicle telemetry | Low | None |
| Maintenance costs | Medium | Financial reporting |
| Customer notes | High | GDPR (PII) |

**Retention Policies:**
- Bronze/Silver: 2 years
- Gold: 5 years
- Maintenance logs: 10 years (regulatory)

---

**Last Updated:** 2026-01-25  
**Version:** 1.0  
**Maintained By:** Data Engineering Team
