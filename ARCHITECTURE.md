# 🏗️ ARCHITECTURE DOCUMENTATION

## Fleet Optimization & Predictive Maintenance Platform

**Author:** Venkat M  
**Date:** 2026-01-26  
**Environment:** Databricks Community Edition

---

## 📊 **Data Architecture Overview**

This project implements a **Medallion Architecture** with three distinct layers for data quality and governance.

### **Architecture Diagram**

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│  • NYC TLC Taxi Trip Data (Realistic Generated Sample)          │
│  • GPS Tracking Data (~2,800 records)                           │
│  • Vehicle Telemetry Data (~2,800 records)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                          │
│  Schema: bronze_schema                                           │
│  Format: Delta Lake                                              │
│  Tables:                                                         │
│    • gps_tracking_raw                                           │
│    • vehicle_telemetry_raw                                      │
│  Purpose: Immutable raw data with audit columns                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleaned)                        │
│  Schema: silver_schema                                           │
│  Format: Delta Lake                                              │
│  Tables:                                                         │
│    • gps_tracking_clean                                         │
│    • vehicle_telemetry_clean                                    │
│  Transformations:                                                │
│    • Data quality validation                                    │
│    • Deduplication                                              │
│    • Anomaly detection                                          │
│    • Derived metrics calculation                                │
│    • Health score computation                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Business)                        │
│  Schema: gold_schema                                             │
│  Format: Delta Lake                                              │
│  Tables:                                                         │
│    • fleet_performance_kpis                                     │
│    • maintenance_prediction_features                            │
│    • vehicle_health_summary                                     │
│  Purpose: Business-ready aggregations and ML features           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MACHINE LEARNING LAYER                        │
│  • Logistic Regression (Baseline)                              │
│  • Random Forest Classifier                                     │
│  • Gradient Boosting Classifier                                │
│  Best Model F1-Score: ~0.88                                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🗄️ **Data Catalog Structure**

### **Current Implementation (Databricks Community Edition)**

```
Hive Metastore (Default)
│
├── bronze_schema/
│   ├── gps_tracking_raw
│   │   ├── Partitioned by: ingestion_date
│   │   ├── Format: Delta Lake
│   │   └── Records: ~2,800
│   │
│   └── vehicle_telemetry_raw
│       ├── Partitioned by: ingestion_date
│       ├── Format: Delta Lake
│       └── Records: ~2,800
│
├── silver_schema/
│   ├── gps_tracking_clean
│   │   ├── Partitioned by: event_date, vehicle_id
│   │   ├── Z-Ordered by: event_timestamp
│   │   ├── Format: Delta Lake
│   │   └── Records: ~2,700 (after cleaning)
│   │
│   └── vehicle_telemetry_clean
│       ├── Partitioned by: event_date, vehicle_id
│       ├── Z-Ordered by: event_timestamp
│       ├── Format: Delta Lake
│       └── Records: ~2,700 (after cleaning)
│
└── gold_schema/
    ├── fleet_performance_kpis
    │   ├── Partitioned by: report_date
    │   ├── Z-Ordered by: vehicle_id
    │   ├── Format: Delta Lake
    │   └── Records: ~100 (daily aggregates)
    │
    ├── maintenance_prediction_features
    │   ├── Partitioned by: feature_date
    │   ├── Z-Ordered by: vehicle_id
    │   ├── Format: Delta Lake
    │   └── Records: ~100 (ML features)
    │
    └── vehicle_health_summary
        ├── No partitions (snapshot table)
        ├── Z-Ordered by: vehicle_id, health_status
        ├── Format: Delta Lake
        └── Records: ~100 (current state)
```

---

## 🏢 **Unity Catalog Considerations**

### **Community Edition Limitations**

**Current Setup:**
- ✅ Uses Hive Metastore (default catalog)
- ✅ Fully functional for development and demonstration
- ❌ Unity Catalog features not available in free tier

### **Production Migration Path**

**When migrating to Databricks Standard/Premium, the structure would be:**

```
Unity Catalog Structure (Production)
│
logistics_catalog/
│
├── bronze_schema/
│   ├── gps_tracking_raw
│   └── vehicle_telemetry_raw
│
├── silver_schema/
│   ├── gps_tracking_clean
│   └── vehicle_telemetry_clean
│
└── gold_schema/
    ├── fleet_performance_kpis
    ├── maintenance_prediction_features
    └── vehicle_health_summary
```

**Migration Benefits:**
- ✅ Centralized governance across workspaces
- ✅ Fine-grained access control (row/column level)
- ✅ Data lineage tracking
- ✅ Audit logging
- ✅ Cross-workspace data sharing
- ✅ Built-in data discovery

**Migration Steps (Future):**
```sql
-- 1. Create Unity Catalog
CREATE CATALOG IF NOT EXISTS logistics_catalog;

-- 2. Create schemas in catalog
CREATE SCHEMA IF NOT EXISTS logistics_catalog.bronze_schema;
CREATE SCHEMA IF NOT EXISTS logistics_catalog.silver_schema;
CREATE SCHEMA IF NOT EXISTS logistics_catalog.gold_schema;

-- 3. Migrate tables (example)
CREATE TABLE logistics_catalog.bronze_schema.gps_tracking_raw
DEEP CLONE bronze_schema.gps_tracking_raw;

-- 4. Update all notebook references
-- Change: bronze_schema.table_name
-- To: logistics_catalog.bronze_schema.table_name
```

---

## 🔒 **Security & Governance**

### **Current Implementation (Hive Metastore)**

**Access Control:**
- Workspace-level permissions
- Schema-level grants
- Table-level grants

**Audit:**
- Cluster logs
- Notebook execution history
- Delta Lake transaction log

### **Production Recommendations (Unity Catalog)**

**Access Control:**
```sql
-- Grant read access to data analysts
GRANT SELECT ON SCHEMA logistics_catalog.gold_schema 
TO `data_analysts`;

-- Grant write access to data engineers
GRANT ALL PRIVILEGES ON SCHEMA logistics_catalog.bronze_schema 
TO `data_engineers`;

-- Row-level security example
CREATE ROW ACCESS POLICY vehicle_access_policy
AS (vehicle_id STRING)
RETURNS BOOLEAN
RETURN current_user() IN (
  SELECT user_email FROM vehicle_ownership 
  WHERE vehicle_id = vehicle_id
);
```

**Data Classification:**
```sql
-- Tag sensitive columns
ALTER TABLE logistics_catalog.silver_schema.gps_tracking_clean
ALTER COLUMN latitude SET TAGS ('PII' = 'location_data');

ALTER TABLE logistics_catalog.silver_schema.gps_tracking_clean
ALTER COLUMN longitude SET TAGS ('PII' = 'location_data');
```

---

## 📈 **Scalability Considerations**

### **Current Scale**
- **Data Volume:** ~5,600 total records
- **Vehicles:** 100
- **Time Period:** 7 days
- **Storage:** ~50 MB

### **Production Scale Estimates**

**For 1,000 vehicles over 1 year:**
```
Bronze Layer:
  • GPS: ~52M records (1,000 vehicles × 365 days × 144 readings/day)
  • Telemetry: ~157M records (1,000 vehicles × 365 days × 432 readings/day)
  • Storage: ~50 GB

Silver Layer:
  • GPS: ~50M records (after cleaning)
  • Telemetry: ~155M records (after cleaning)
  • Storage: ~45 GB

Gold Layer:
  • KPIs: ~365K records (1,000 vehicles × 365 days)
  • ML Features: ~365K records
  • Health Summary: ~1K records (current snapshot)
  • Storage: ~2 GB
```

**Optimization Strategies:**
1. **Partitioning:** By date and vehicle_id
2. **Z-Ordering:** On frequently filtered columns
3. **Compaction:** Regular OPTIMIZE operations
4. **Retention:** 30-day retention for Bronze, 90-day for Silver
5. **Archival:** Move old data to cold storage (S3 Glacier)

---

## 🔄 **Data Pipeline Flow**

### **Batch Processing Schedule**

```
Daily Pipeline (Production):
├── 00:00 - Ingest raw data (Bronze)
├── 01:00 - Transform to Silver
├── 02:00 - Aggregate to Gold
├── 03:00 - Update ML features
├── 04:00 - Run predictions
└── 05:00 - Generate reports
```

### **Data Freshness SLAs**

| Layer | Freshness | Update Frequency |
|-------|-----------|------------------|
| Bronze | Real-time | Streaming (5 min) |
| Silver | < 15 min | Micro-batch (15 min) |
| Gold | < 1 hour | Batch (hourly) |
| ML Predictions | < 2 hours | Batch (2 hours) |

---

## 🛠️ **Technology Stack**

### **Data Platform**
- **Compute:** Databricks Runtime 14.3 LTS
- **Storage:** Delta Lake 3.0
- **Catalog:** Hive Metastore (Community) → Unity Catalog (Production)
- **Language:** Python 3.11, SQL

### **Data Processing**
- **Framework:** Apache Spark 3.5
- **API:** PySpark DataFrame API
- **Format:** Delta Lake (Parquet + Transaction Log)

### **Machine Learning**
- **Framework:** scikit-learn 1.3
- **Tracking:** MLflow (disabled in Community Edition)
- **Models:** Logistic Regression, Random Forest, Gradient Boosting

### **Optimization**
- **Partitioning:** Date-based and vehicle_id
- **Z-Ordering:** Multi-column clustering
- **Compaction:** Auto-optimize enabled
- **Caching:** Adaptive Query Execution (AQE)

---

## 📊 **Data Quality Framework**

### **Bronze Layer**
- ✅ Schema validation
- ✅ Audit columns (_ingestion_time, _source_file)
- ✅ Immutable (append-only)

### **Silver Layer**
- ✅ Null handling
- ✅ Duplicate removal
- ✅ Outlier detection
- ✅ Anomaly flagging
- ✅ Data quality scores (0-1)

### **Gold Layer**
- ✅ Business rule validation
- ✅ Referential integrity
- ✅ Aggregation accuracy
- ✅ Feature completeness

---

## 🎯 **Key Design Decisions**

### **1. Medallion Architecture**
**Decision:** Use Bronze-Silver-Gold layers  
**Rationale:** 
- Separation of concerns
- Data quality progression
- Reusability across use cases
- Industry best practice

### **2. Delta Lake Format**
**Decision:** Use Delta Lake for all tables  
**Rationale:**
- ACID transactions
- Time travel capabilities
- Schema evolution
- Efficient upserts/deletes

### **3. Partitioning Strategy**
**Decision:** Partition by date and vehicle_id  
**Rationale:**
- Common query patterns (date range + vehicle)
- Partition pruning optimization
- Manageable partition sizes

### **4. Z-Ordering**
**Decision:** Z-Order on timestamp and vehicle_id  
**Rationale:**
- Improves query performance
- Co-locates related data
- Reduces data scanning

---

## 📚 **References & Standards**

### **Databricks Best Practices**
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

### **Data Governance**
- GDPR compliance for location data
- Data retention policies
- Access control standards

---

## 🚀 **Future Enhancements**

### **Short-term (3-6 months)**
1. ✅ Migrate to Unity Catalog
2. ✅ Implement streaming ingestion
3. ✅ Add real-time dashboards
4. ✅ Deploy ML model to production

### **Long-term (6-12 months)**
1. ✅ Multi-region deployment
2. ✅ Advanced ML models (Deep Learning)
3. ✅ Predictive route optimization
4. ✅ Integration with IoT devices

---

**Last Updated:** 2026-01-26  
**Version:** 1.0  
**Status:** Production-Ready (Community Edition)
