# 🚍 Smart Budapest Mobility Analytics Platform

A comprehensive Business Intelligence system that integrates real-time public transport data with weather conditions to analyze mobility patterns in Budapest. Built with modern data engineering tools and containerized infrastructure.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Setup Instructions](#setup-instructions)
- [Project Structure](#project-structure)
- [Data Warehouse Schema](#data-warehouse-schema)
- [ETL Pipelines](#etl-pipelines)
- [Dashboards & KPIs](#dashboards--kpis)
- [Machine Learning](#machine-learning)
- [Known Limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This project implements a complete data analytics pipeline to process and visualize Budapest's public transportation data. It combines real-time vehicle tracking from BKK Futár API with weather data from OpenWeatherMap to identify correlations between environmental conditions and transport performance.

**Key Features:**
- Real-time ingestion of 850+ vehicles every 10 minutes
- Hourly weather data collection and correlation analysis
- Star schema data warehouse with 2M+ records
- Automated ETL pipelines orchestrated with Apache Airflow
- Interactive dashboards for transport and weather insights

---

## 🏗️ Architecture

```
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Data Sources    │   │ Orchestration   │   │ Presentation    │
├─────────────────┤   ├─────────────────┤   ├─────────────────┤
│ BKK Futár API   │──▶│ Apache Airflow  │──▶│ Metabase       │
│ OpenWeatherMap  │   │ (3 DAGs)        │   │ (Dashboards)    │
│ GTFS Static     │   │                 │   │                 │
└─────────────────┘   └─────────────────┘   └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ PostgreSQL      │
                    │ Data Warehouse  │
                    │ (Star Schema)   │
                    └─────────────────┘
```

**Flow:**
1. **Extract:** Python scripts fetch data from APIs (BKK every 10min, Weather hourly)
2. **Transform:** Data is cleaned, validated, and shaped for dimensional modeling
3. **Load:** Staged data populates fact and dimension tables in PostgreSQL
4. **Orchestrate:** Airflow schedules and monitors all pipeline executions
5. **Visualize:** Metabase connects to the DWH for real-time analytics

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Warehouse** | TimescaleDB (PostgreSQL 15) | Time-series optimized data storage |
| **Orchestration** | Apache Airflow 2.7 | ETL scheduling and monitoring |
| **Visualization** | Metabase Latest | Business intelligence dashboards |
| **ETL Language** | Python 3.11 | Data extraction and transformation |
| **Containerization** | Docker + Docker Compose | Infrastructure as code |
| **Libraries** | psycopg2, requests, pandas, pytest | Database, API, data processing, and testing |

---

## 📊 Data Sources

### 1. BKK Futár API (Real-Time Transport)
- **Endpoint:** `https://futar.bkk.hu/api/query/v1/ws/otp/api/where/vehicles-for-location.json`
- **Frequency:** Every 10 minutes
- **Data:** Vehicle positions, routes, delays, bearings
- **Volume:** ~850 vehicles per request

### 2. OpenWeatherMap API (Weather Conditions)
- **Endpoint:** `https://api.openweathermap.org/data/2.5/weather`
- **Frequency:** Hourly
- **Data:** Temperature, humidity, wind speed, weather codes
- **Location:** Budapest (lat: 47.4979, lon: 19.0402)

### 3. GTFS Static Data (Transport Reference)
- **Source:** BKK GTFS feed (manual download)
- **Format:** CSV files (routes, stops, trips, schedules)
- **Purpose:** Reference data for routes and stops
- **Load:** One-time initial setup

---

## 🚀 Setup Instructions

### Prerequisites
- Docker Desktop installed and running
- Git installed
- 8GB RAM available
- Windows 10/11 or Linux/macOS

### Installation Steps

**1. Clone the repository:**
```bash
git clone https://github.com/eneko-alvarez/smart-budapest-mobility.git
cd smart-budapest-mobility
```

**2. Create `.env` file with your API keys:**
```env
# Database Configuration
DB_HOST=bi_postgres
DB_PORT=5432
DB_USER=bi_user
DB_PASSWORD=bi_password_secure
DB_NAME=bi_budapest

# API Keys
BKK_API_KEY=your_bkk_api_key
OPENWEATHER_API_KEY=your_openweather_api_key
```

**3. Start the infrastructure:**
```bash
docker-compose up -d
```

Wait 2-3 minutes for all services to initialize.

**4. Initialize the database schema:**
```bash
docker exec -it bi_postgres psql -U bi_user -d bi_budapest -f /docker-entrypoint-initdb.d/01_schema.sql
```

**5. Load GTFS static data:**
```bash
# Download GTFS from BKK website and place in data/gtfs/
python python/extractors/gtfs_extractor.py
python python/transformers/gtfs_transformer.py
```

**6. Access the services:**
- **Airflow:** http://localhost:8080 (user: `admin`, pass: `admin`)
- **Metabase:** http://localhost:3000 (setup on first visit)
- **PostgreSQL:** localhost:5432 (user: `bi_user`)

**7. Activate DAGs in Airflow:**
- Enable `bkk_realtime_pipeline`
- Enable `weather_hourly_pipeline`
- Enable `daily_kpi_aggregation`

---

## 📁 Project Structure

```
smart-budapest-mobility/
│
├── airflow/                             # Airflow orchestration: DAGs and logs
│   ├── dags/
│   │   ├── bkk_realtime_pipeline.py    # ETL pipeline for BKK Futár real-time transport data
│   │   ├── kpi_daily_dag.py            # Daily KPIs aggregation pipeline
│   │   └── weather_pipeline_dag.py     # Hourly weather data pipeline
│   └── logs/                           # Execution logs for Airflow DAGs
│
├── data/                               # Static and downloaded datasets
│   ├── budapest_gtfs.zip               # Compressed full Budapest GTFS feed
│   └── gtfs/                          # Decompressed GTFS files (routes, stops, etc.)
│       ├── agency.txt                 
│       ├── calendar_dates.txt
│       ├── feed_info.txt
│       ├── pathways.txt
│       ├── routes.txt
│       ├── shapes.txt
│       ├── stops.txt
│       ├── stop_times.txt
│       └── trips.txt
│
├── docker/                            # Dockerfiles and base requirements
│   ├── Dockerfile.airflow             # Custom Airflow image definition
│   └── requirements.txt               # Python dependencies for Airflow container
│
├── docs/                              # Additional documentation and diagrams
│
├── logs/                              # Application logs
│
├── models/                            # ML model artifacts (currently empty - future work)
│
├── python/                            # Main Python codebase
│   ├── extractors/                    # API and file data extractors
│   │   ├── bkk_futar_extractor.py    # BKK Futár API real-time data extraction
│   │   ├── gtfs_loader.py            # GTFS static data loader
│   │   └── weather_extractor.py      # OpenWeatherMap API extraction
│   ├── ml/                           # Machine learning modules (incomplete - see limitations)
│   │   ├── predict_transport_demand.py  # Demand prediction (requires more data)
│   │   └── train_demand_forecast.py     # Model training scripts
│   ├── transformers/                 # Data transformation and loading
│   │   ├── bkk_transformer.py        # Transform BKK data to DWH format
│   │   ├── daily_kpi_transformer.py  # Calculate daily KPIs
│   │   ├── dim_time_loader.py        # Populate time dimension
│   │   ├── gtfs_transformer.py       # Transform GTFS to dimensions
│   │   ├── kpi_daily_loader.py       # Load daily KPIs to DWH
│   │   ├── weather_raw_loader.py     # Load weather to staging
│   │   ├── weather_to_fact.py        # Transform weather to fact table
│   │   └── weather_to_raw.py         # Weather staging transformation
│   └── utils/                        # Utility scripts
│       └── check_connectivity.py     # Database connection verification
│
├── sql/                                      # SQL scripts for database initialization
│   ├── 001_create_schemas.sql               # Create schemas (staging, raw, dwh, metadata)
│   ├── 002_create_staging.sql               # Staging tables with TimescaleDB hypertables
│   ├── 003_create_raw.sql                   # Raw data tables with retention policies
│   ├── 004_create_dwh.sql                   # Data warehouse star schema (dims + facts)
│   ├── 005_create_fact_route_performance.sql # Route performance fact table
│   └── 005_policies_and_indexes.sql         # Retention policies and performance indexes
│
├── .env                              # Environment variables (API keys, DB credentials)
├── .env.example                      # Template for environment variables
├── .gitignore                        # Git ignore configuration
├── docker-compose.yml                # Docker services: TimescaleDB, Airflow, Metabase
├── requirements.txt                  # Python dependencies
└── README.md                         # This file           
```

---

## 🗄️ Data Warehouse Schema

### Star Schema Design

**Fact Tables:**
- `fact_transport_usage` - Vehicle events with delays, routes, and time (real-time data)
- `fact_weather_conditions` - Hourly weather observations
- `fact_route_performance` - Daily aggregated KPIs by route (trips, unique vehicles)
- `kpi_daily` - Daily metrics aggregation table
- `correlation_results` - Weather-transport correlation analysis results

**Dimension Tables:**
- `dim_time` - Date/hour hierarchy
- `dim_route` - Bus/tram/metro routes
- `dim_stop` - Public transport stops
- `dim_weather_type` - Weather condition codes

### Key Relationships

```
fact_transport_usage
├─ time_key → dim_time.time_key
├─ route_key → dim_route.route_key
└─ stop_key → dim_stop.stop_key (currently NULL - see limitations)

fact_weather_conditions
├─ time_key → dim_time.time_key
└─ weather_key → dim_weather_type.weather_key

fact_route_performance
├─ time_key → dim_time.time_key
└─ route_key → dim_route.route_key
```

---

## 🔄 ETL Pipelines

### 1. BKK Real-Time Pipeline
**Schedule:** Every 10 minutes  
**DAG ID:** `bkk_realtime_pipeline`

**Steps:**
1. **Extract:** Fetch vehicle positions from BKK Futár API (`bkk_futar_extractor.py`)
2. **Load to Staging:** Insert raw JSON into `staging.bkk_futar_raw` (TimescaleDB hypertable)
3. **Transform:** (`bkk_transformer.py`)
   - Parse JSON fields (latitude, longitude, bearing, delay, speed)
   - Strip `BKK_` prefix from route_id for matching
   - Match route_id to `dim_route.route_key`
   - Create time_key (hourly granularity) from timestamp
   - Calculate speed in km/h from vehicle data
4. **Load to DWH:** Insert into `fact_transport_usage`
5. **Cleanup:** Automatic retention policy (7 days for staging)

**Output:** ~850 vehicle records per execution  
**Data Retention:** Staging data retained for 7 days, DWH data indefinitely

---

### 2. Weather Hourly Pipeline
**Schedule:** Hourly (at minute 0)  
**DAG ID:** `weather_pipeline_dag`

**Steps:**
1. **Extract:** Call OpenWeatherMap API for Budapest coordinates (`weather_extractor.py`)
2. **Load to Staging:** Insert raw JSON into `staging.weather_raw` (TimescaleDB hypertable)
3. **Transform:** (`weather_to_raw.py`, `weather_to_fact.py`)
   - Extract temperature (°C), humidity (%), wind speed (m/s)
   - Map weather code to `dim_weather_type.weather_key`
   - Create time_key for current hour
4. **Load to DWH:** Insert into `fact_weather_conditions`

**Output:** 1 weather record per execution  
**Data Retention:** Staging 7 days, raw 180 days

---

### 3. Daily KPI Aggregation
**Schedule:** Daily at 1:00 AM  
**DAG ID:** `kpi_daily_dag`

**Steps:**
1. **Aggregate:** Query `fact_transport_usage` for previous day (`daily_kpi_transformer.py`)
2. **Calculate KPIs:**
   - Total trips per route
   - Unique vehicles per route
   - Average delay (seconds) - currently ~0 for Budapest
   - Total delay (seconds)
3. **Load:** Insert into `fact_route_performance` and `dwh.kpi_daily`

**Output:** ~180 route-day combinations  
**Deduplication:** Uses `ON CONFLICT` to prevent duplicate daily records

---

## 📈 Dashboards & KPIs

The project includes three comprehensive Metabase dashboards for real-time analytics:

### Dashboard 1: Weather Overview 🌤️
**Purpose:** Monitor Budapest's environmental conditions and trends  
**Refresh:** Hourly

**Key Visualizations:**
1. **Current Weather Conditions** - Real-time temperature, humidity, wind speed
2. **Temperature Trends** - Line chart showing temperature evolution over time
3. **Weather Distribution** - Pie chart of weather condition types (Clear, Clouds, Rain, etc.)
4. **Humidity Analysis** - Humidity percentage trends
5. **Wind Speed Patterns** - Wind speed variations throughout the day

**Data Source:** `dwh.fact_weather_conditions` joined with `dim_weather_type` and `dim_time`

---

### Dashboard 2: Real-Time Transport Performance 🚍
**Purpose:** Monitor Budapest's public transport fleet and route efficiency  
**Refresh:** Every 10 minutes

**Key Visualizations:**
1. **Active Vehicles Now** - KPI card showing current fleet size
2. **Active Routes Today** - Number of routes currently operating
3. **Vehicles by Hour** - Line chart showing fleet distribution throughout the day
4. **Top 10 Most Active Routes** - Bar chart of routes with most vehicles
5. **Route Type Distribution** - Breakdown by bus, tram, metro, etc.
6. **Vehicle Speed Analysis** - Average speed by route and time
7. **Delay Analysis** - Average delay in seconds (typically near 0 for Budapest)

**Key Metrics:**
- Active vehicles: ~850 vehicles at peak times
- Active routes: ~180 routes/day
- Average delay: ~0 seconds (Budapest has highly efficient transport)
- Peak hours: 7-9 AM, 4-7 PM weekdays

**Data Source:** `dwh.fact_transport_usage` joined with `dim_route` and `dim_time`

---

### Dashboard 3: Transport-Weather Correlation 🌡️🚌
**Purpose:** Analyze the relationship between weather conditions and transport usage  
**Refresh:** Hourly

**Key Visualizations:**
1. **Transport Usage vs Temperature** - Dual-axis line chart correlating vehicle count with temperature
2. **Hourly Activity Patterns** - Transport usage distribution by hour of day
3. **Daily Transport Trends** - Multi-day comparison of transport activity
4. **Weather Impact on Routes** - Route performance under different weather conditions
5. **Weekly Patterns** - Weekday vs weekend transport usage

**Insights:**
- Transport usage peaks during rush hours regardless of weather
- Slight increase in vehicle count during adverse weather conditions
- Weekend patterns show different peak times (10 AM - 8 PM)

**Data Source:** Combined queries from `fact_transport_usage`, `fact_weather_conditions`, and dimension tables

**Note:** Correlation analysis is limited by weather data collection period. More historical data will improve insights over time.

---

---

## 🤖 Machine Learning

### Current Status: Incomplete ⚠️

The project includes a Machine Learning module structure (`python/ml/`) with planned predictive analytics features. However, **ML models are not yet fully implemented** due to insufficient training data.

### Planned Features

#### 1. Transport Demand Forecasting
**Goal:** Predict vehicle demand by route based on historical patterns, weather, and time factors

**Approach:**
- Time series forecasting using historical `fact_transport_usage` data
- Feature engineering: hour of day, day of week, weather conditions, holidays
- Models: ARIMA, Prophet, or LSTM for seasonal patterns

**Requirements:** Minimum 6-12 months of data for seasonal pattern recognition

#### 2. "Cursed Routes" Prediction
**Goal:** Identify routes likely to experience congestion or delays

**Approach:**
- Classification model to predict high-delay routes
- Features: weather conditions, time of day, historical speed data
- Output: Risk score for each route-time combination

**Requirements:** Sufficient delay variance in data (currently Budapest has minimal delays)

#### 3. Weather-Transport Correlation Analysis
**Goal:** Quantify the impact of weather on transport usage

**Approach:**
- Statistical correlation analysis (Pearson, Spearman)
- Regression models to predict usage changes based on weather
- Store results in `dwh.correlation_results` table

**Requirements:** Aligned weather and transport data for same time periods

### Why ML is Incomplete

1. **Insufficient Data Volume:** Only ~2-3 weeks of data collected; ML models need 6-12 months
2. **Limited Weather Coverage:** Weather data collection started later than transport data
3. **Low Delay Variance:** Budapest's efficient transport system has minimal delays (~0 seconds average), making delay prediction less meaningful
4. **Seasonal Patterns Missing:** Need full year of data to capture seasonal trends (summer vs winter usage)

### Code Structure

```
python/ml/
├── train_demand_forecast.py     # Model training pipeline (placeholder)
└── predict_transport_demand.py  # Prediction inference (placeholder)
```

### Future Work

Once sufficient data is collected (target: 12 months), the following steps will be taken:

1. **Data Preparation:** Create ML-ready datasets with engineered features
2. **Model Training:** Train and validate forecasting models
3. **Model Deployment:** Integrate predictions into Airflow DAGs
4. **Dashboard Integration:** Add prediction visualizations to Metabase
5. **Continuous Learning:** Implement model retraining pipeline

**Estimated Timeline:** 6-12 months of data collection required before ML implementation

---

## ⚠️ Known Limitations

### 1. Stop Key Null Values
**Issue:** `fact_transport_usage.stop_key` is NULL for all records  
**Cause:** BKK Futár API does not provide `stop_id` for vehicles in motion  
**Impact:** Cannot analyze performance by specific bus stops  
**Workaround:** Use route-level aggregations instead  
**Status:** API limitation - cannot be resolved without additional data source

### 2. Limited Weather Data for ML
**Issue:** Insufficient historical weather data for robust correlation analysis  
**Cause:** Weather pipeline started later than transport pipeline  
**Impact:** Limited statistical significance in weather-transport correlations  
**Solution:** Continue data collection; ML models require minimum 6-12 months of data  
**Current Status:** Ongoing data collection

### 3. Machine Learning Module Incomplete
**Issue:** ML prediction models not fully implemented  
**Cause:** Insufficient training data (need minimum 6 months for seasonal patterns)  
**Impact:** Cannot predict transport demand or identify "cursed routes" yet  
**Planned Features:**
  - Demand forecasting based on weather and time
  - Route congestion prediction
  - Anomaly detection for delays
**Status:** Code structure in place (`python/ml/`), awaiting sufficient data

### 4. Redis Service Unused
**Issue:** Redis container runs but is not used by the application  
**Cause:** Originally planned for caching but not implemented  
**Impact:** Minor resource overhead (~50MB RAM)  
**Status:** Can be removed from `docker-compose.yml` if needed

---

## 🐛 Troubleshooting

### Airflow DAGs not running
**Symptom:** DAGs show "Queued" but never execute  
**Solution:**
```bash
docker-compose restart airflow-scheduler
docker-compose logs -f airflow-scheduler
```

### No data in dashboards
**Symptom:** Queries return 0 rows  
**Solution:** Wait 30 minutes for pipelines to run, then check:
```sql
SELECT COUNT(*) FROM dwh.fact_transport_usage;  -- Should be >0
SELECT COUNT(*) FROM dwh.fact_weather_conditions;  -- Should be >0
```

---

## 📚 Additional Resources

- **BKK Futár API Documentation:** https://bkkfutar.docs.apiary.io/
- **OpenWeatherMap API Docs:** https://openweathermap.org/api
- **GTFS Reference:** https://gtfs.org/reference/static
- **Airflow Documentation:** https://airflow.apache.org/docs/
- **Metabase User Guide:** https://www.metabase.com/docs/

---

## 👤 Author
Eneko Alvarez 
University: Budapesti Műszaki és Gazdaságtudományi Egyetem  
Academic Year: 2024-2025

---

## 📄 License

This project is developed for educational purposes as part of a Business Intelligence course assignment.