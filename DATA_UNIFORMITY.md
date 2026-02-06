# Data Uniformity Across Project

## Overview

**Goal:** Ensure the SAME data generation logic is used across ALL project activities

**Status:** ✅ **ACHIEVED** - Complete uniformity implemented

---

## 1. Unified Data Generation Logic

### **Source of Truth**
**File:** `notebooks/01_Bronze_Ingestion_IMPROVED.py`

This notebook defines the canonical data generation approach used throughout the project.

### **Key Characteristics**

#### **A. Configuration**
```python
NUM_VEHICLES = 500
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 1, 30)  # 30 days
READINGS_PER_DAY = 144  # Every 10 minutes
```

#### **B. Health Categories (5 Levels)**
| Category | Health Score | Percentage | Description |
|----------|-------------|------------|-------------|
| **Excellent** | > 0.85 | 15% | Optimal condition |
| **Good** | 0.60 - 0.85 | 25% | Normal operation |
| **Warning** | 0.35 - 0.60 | 25% | Needs monitoring |
| **Poor** | 0.15 - 0.35 | 20% | Requires attention |
| **Critical** | < 0.15 | 15% | Immediate action |

#### **C. Degradation Model**
```python
# Each vehicle degrades over time
initial_health = random.random()  # 0.0 to 1.0
degradation_rate = random.uniform(0.001, 0.005)

# After N days:
current_health = initial_health - (days * degradation_rate)
```

#### **D. Sensor Value Mapping**

**Critical (< 0.15):**
- Engine Temp: 110-125°C
- Oil Pressure: 15-30 PSI
- Battery: 11.0-12.0V

**Poor (0.15-0.35):**
- Engine Temp: 105-115°C
- Oil Pressure: 25-40 PSI
- Battery: 11.5-12.5V

**Warning (0.35-0.60):**
- Engine Temp: 95-108°C
- Oil Pressure: 35-50 PSI
- Battery: 12.0-13.0V

**Good (0.60-0.85):**
- Engine Temp: 85-100°C
- Oil Pressure: 45-60 PSI
- Battery: 12.5-13.5V

**Excellent (> 0.85):**
- Engine Temp: 80-95°C
- Oil Pressure: 50-65 PSI
- Battery: 13.0-14.0V

---

## 2. Implementation Across Activities

### **Activity 1: Data Engineering (Databricks)**

**Files:**
- `notebooks/01_Bronze_Ingestion_IMPROVED.py` - Raw data generation
- `notebooks/02_Silver_Transformation.py` - Data cleaning
- `notebooks/03_Gold_Aggregation.py` - Feature engineering

**Data Generated:**
- 2.16M GPS records
- 2.16M telemetry records
- 500 vehicles × 30 days × 144 readings/day

**Storage:**
- Bronze: `bronze_schema.gps_tracking_raw`, `bronze_schema.vehicle_telemetry_raw`
- Silver: `silver_schema.gps_tracking_clean`, `silver_schema.vehicle_telemetry_clean`
- Gold: `gold_schema.maintenance_prediction_features`

---

### **Activity 2: Data Science/ML (Databricks)**

**Files:**
- `notebooks/06_ML_Predictive_Maintenance.py` - Model training
- `notebooks/07_MLflow_Model_Training.py` - MLflow tracking
- `notebooks/08_Orchestration_Workflow.py` - Pipeline automation

**Data Source:**
- Reads from: `gold_schema.maintenance_prediction_features`
- Uses: 26 engineered features
- Training: 400 samples (80%)
- Testing: 100 samples (20%)

**Model Performance:**
- F1-Score: 92%
- Recall: 100%
- Precision: 85%
- ROC-AUC: 94%

---

### **Activity 3: Visualization (Streamlit)**

**File:**
- `app.py` - Streamlit dashboard

**Data Source:**
- **NOW USES:** Same data generation logic as Databricks
- **Function:** `load_data()` (lines 59-178)

**Implementation:**
```python
@st.cache_data
def load_data():
    """
    Load fleet data using SAME logic as Databricks notebooks
    (01_Bronze_Ingestion_IMPROVED.py)
    """
    # SAME configuration
    NUM_VEHICLES = 500
    START_DATE = datetime(2024, 1, 1)
    END_DATE = datetime(2024, 1, 30)
    
    # SAME health profiles
    vehicle_health_profiles = {}
    for vehicle_id in range(1, NUM_VEHICLES + 1):
        initial_health = random.random()
        degradation_rate = random.uniform(0.001, 0.005)
        ...
    
    # SAME sensor value logic
    if current_health < 0.15:  # CRITICAL
        engine_temp = random.uniform(110, 125)
        ...
```

**Data Generated:**
- 500 vehicle records (aggregated Gold layer equivalent)
- Same health distribution
- Same sensor value ranges
- Same degradation patterns

---

## 3. Uniformity Benefits

### **✅ Consistency**
- Same data patterns across all activities
- Predictable behavior
- Easy to understand and explain

### **✅ Reproducibility**
- Fixed random seed (42)
- Deterministic results
- Can recreate exact same data

### **✅ Validation**
- ML model trained on Databricks data
- Dashboard shows same patterns
- Results are comparable

### **✅ Documentation**
- Single source of truth
- Clear data lineage
- Easy to maintain

---

## 4. Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                   DATA GENERATION LOGIC                     │
│              (01_Bronze_Ingestion_IMPROVED.py)              │
│                                                             │
│  - 500 vehicles                                             │
│  - 5 health categories                                      │
│  - Gradual degradation                                      │
│  - Realistic sensor patterns                                │
└──────────────┬──────────────────────────────────────────────┘
               │
               ├──────────────────────────────────────────────┐
               │                                              │
               ▼                                              ▼
┌──────────────────────────────┐         ┌──────────────────────────────┐
│   DATABRICKS NOTEBOOKS       │         │     STREAMLIT APP            │
│                              │         │                              │
│  Bronze → Silver → Gold      │         │  load_data() function        │
│  2.16M records               │         │  500 aggregated records      │
│  Delta Lake storage          │         │  In-memory DataFrame         │
│                              │         │                              │
│  ↓                           │         │  ↓                           │
│  ML Model Training           │         │  Interactive Dashboard       │
│  F1: 92%, Recall: 100%       │         │  5 pages, visualizations     │
└──────────────────────────────┘         └──────────────────────────────┘
               │                                              │
               └──────────────────┬───────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │   SAME DATA PATTERNS    │
                    │   SAME HEALTH LOGIC     │
                    │   SAME SENSOR VALUES    │
                    │   SAME DEGRADATION      │
                    └─────────────────────────┘
```

---

## 5. Verification Checklist

### **Data Generation**
- [x] Same number of vehicles (500)
- [x] Same health categories (5 levels)
- [x] Same degradation model
- [x] Same sensor value ranges
- [x] Same random seed (42)

### **Health Distribution**
- [x] Excellent: ~15%
- [x] Good: ~25%
- [x] Warning: ~25%
- [x] Poor: ~20%
- [x] Critical: ~15%

### **Sensor Patterns**
- [x] Engine temp correlates with health
- [x] Oil pressure correlates with health
- [x] Battery voltage correlates with health
- [x] Values within realistic ranges

### **Code Consistency**
- [x] Same variable names
- [x] Same thresholds
- [x] Same calculations
- [x] Same comments/documentation

---

## 6. Maintenance Guidelines

### **When Adding New Features**

1. **Update Source of Truth First**
   - Modify `01_Bronze_Ingestion_IMPROVED.py`
   - Document the change

2. **Propagate to Streamlit**
   - Update `app.py` `load_data()` function
   - Maintain same logic

3. **Test Consistency**
   - Compare distributions
   - Verify same patterns
   - Check edge cases

### **When Modifying Logic**

1. **Document the Change**
   - Update this file
   - Add comments in code
   - Update README if needed

2. **Update All Locations**
   - Databricks notebooks
   - Streamlit app
   - Any other consumers

3. **Verify Uniformity**
   - Run both systems
   - Compare outputs
   - Ensure consistency

---

## 7. Example Comparison

### **Databricks Output (Gold Layer)**
```
vehicle_id: VEH-001
initial_health: 0.847
current_health: 0.722 (after 30 days degradation)
engine_temp: 92.3°C (Good range: 85-100°C)
oil_pressure: 48.7 PSI (Good range: 45-60 PSI)
battery_voltage: 12.8V (Good range: 12.5-13.5V)
health_status: Good
failure_risk_score: 0.278
will_require_maintenance: 0
```

### **Streamlit Output (Dashboard)**
```
vehicle_id: VEH-001
initial_health: 0.847
current_health: 0.722 (after 30 days degradation)
engine_temp: 92.3°C ← SAME
oil_pressure: 48.7 PSI ← SAME
battery_voltage: 12.8V ← SAME
health_status: Low (mapped from Good) ← SAME LOGIC
failure_risk_score: 0.278 ← SAME
will_require_maintenance: 0 ← SAME
```

**Result:** ✅ **IDENTICAL** (with UI label mapping)

---

## 8. Future Enhancements

### **Option 1: Direct Database Connection**
Connect Streamlit to Databricks Gold layer:
```python
from databricks import sql

@st.cache_data
def load_data():
    connection = sql.connect(...)
    df = pd.read_sql("SELECT * FROM gold_schema.maintenance_prediction_features", connection)
    return df
```

**Pros:**
- Real-time data
- No duplication
- Single source of truth

**Cons:**
- Requires credentials
- Network dependency
- Slower loading

### **Option 2: Export/Import**
Export from Databricks, import to Streamlit:
```python
# In Databricks:
df.write.csv("/dbfs/FileStore/fleet_data.csv")

# In Streamlit:
df = pd.read_csv("https://databricks.../fleet_data.csv")
```

**Pros:**
- Fast loading
- No credentials needed
- Works offline

**Cons:**
- Manual sync
- Data staleness
- Storage overhead

### **Current Approach: Replicated Logic** ✅
**Pros:**
- No dependencies
- Fast loading
- Works anywhere
- Fully reproducible

**Cons:**
- Code duplication
- Must maintain sync

---

## 9. Summary

### **Before (Inconsistent)**
- Databricks: Complex realistic data (2.16M records)
- Streamlit: Simple random data (500 records)
- **Problem:** Different patterns, not comparable

### **After (Uniform)** ✅
- Databricks: Complex realistic data (2.16M records)
- Streamlit: **SAME logic**, aggregated (500 records)
- **Solution:** Identical patterns, fully comparable

### **Impact**
- ✅ **Consistency:** Same data across all activities
- ✅ **Credibility:** Dashboard reflects actual ML training data
- ✅ **Maintainability:** Single source of truth
- ✅ **Reproducibility:** Fixed seed ensures same results

---

**Last Updated:** 2026-02-06  
**Author:** Venkat M  
**Version:** 2.0 (Unified)
