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
| **Data Warehouse** | PostgreSQL 16 | Dimensional data storage |
| **Orchestration** | Apache Airflow 2.7 | ETL scheduling and monitoring |
| **Visualization** | Metabase Latest | Business intelligence dashboards |
| **ETL Language** | Python 3.11 | Data extraction and transformation |
| **Containerization** | Docker + Docker Compose | Infrastructure as code |
| **Libraries** | psycopg2, requests, python-dotenv | Database and API connectivity |

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
│   │   └── weather_pipeline_dag.py    # Hourly weather data pipeline
│   └── logs/                          # Execution logs for Airflow DAGs
│
├── data/                              # Static and downloaded datasets
│   ├── budapest_gtfs.zip              # Compressed full Budapest GTFS feed
│   └── gtfs/                         # Decompressed GTFS files (routes, stops, etc.)
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
├── docker/                           # Dockerfiles and base requirements
│   ├── Dockerfile.airflow            # Custom Airflow image definition
│   └── requirements.txt              # Base Python dependencies
│
├── docs/                            # Additional documentation and diagrams (PDFs, images)
│
├── python/                          # Main python code split by function
│   ├── extractors/                  # API and file data extractors
│   │   ├── bkk_futar_extractor.py
│   │   ├── gtfs_loader.py
│   │   └── weather_extractor.py
│   ├── ml/                         # Machine learning code or placeholders for advanced features
│   ├── transformers/               # Data cleansing, transformation, and loading scripts
│   │   ├── bkk_transformer.py
│   │   ├── daily_kpi_transformer.py
│   │   ├── dim_time_loader.py
│   │   ├── gtfs_transformer.py
│   │   ├── kpi_daily_loader.py
│   │   ├── weather_raw_loader.py
│   │   ├── weather_to_fact.py
│   │   └── weather_to_raw.py
│   ├── utils/                      # Utility scripts (connection checks, etc.)
│   │   └── check_connectivity.py
│   ├── validators/                # Custom validation scripts if any
│
├── sql/                           # SQL scripts for DB schema and config setup
│   ├── 001_create_schemas.sql
│   ├── 002_create_staging.sql
│   ├── 003_create_raw.sql
│   ├── 004_create_dwh.sql
│   └── 005_policies_and_indexes.sql  # Partition policies and optimization indexes
│
├── .env                           # Environment variables file (API keys hidden on sharing)
├── .env.example                   # Template for environment variables
├── .gitignore                    # Git ignore configuration
├── docker-compose.yml             # Docker service definitions (PostgreSQL, Airflow, Metabase)
├── requirements.txt              # Python dependencies general
├── backups/                      # Manual backups of DB, Metabase, environment
│   ├── *.dump
│   ├── *.backup
│   └── env_backup
│
└── README.md           
```

---

## 🗄️ Data Warehouse Schema

### Star Schema Design

**Fact Tables:**
- `fact_transport_usage` - Vehicle events with delays, route, time
- `fact_weather_conditions` - Hourly weather observations
- `fact_route_performance` - Daily aggregated KPIs by route

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
1. **Extract:** Fetch vehicle positions from BKK Futár API
2. **Load to Staging:** Insert raw JSON into `staging.bkk_vehicles_raw`
3. **Transform:** 
   - Parse JSON fields (lat, lon, bearing, delay)
   - Match route_id to `dim_route`
   - Create time_key (hourly granularity)
4. **Load to DWH:** Insert into `fact_transport_usage`
5. **Cleanup:** Truncate staging table

**Output:** ~850 records per execution

---

### 2. Weather Hourly Pipeline
**Schedule:** Hourly (on the hour)  
**DAG ID:** `weather_hourly_pipeline`

**Steps:**
1. **Extract:** Call OpenWeatherMap API for Budapest
2. **Load to Staging:** Insert raw JSON into `staging.weather_raw`
3. **Transform:**
   - Extract temperature, humidity, wind speed
   - Map weather code to `dim_weather_type`
   - Create time_key for current hour
4. **Load to DWH:** Insert into `fact_weather_conditions`

**Output:** 1 record per execution

---

### 3. Daily KPI Aggregation
**Schedule:** Daily at 1:00 AM  
**DAG ID:** `daily_kpi_aggregation`

**Steps:**
1. **Aggregate:** Query `fact_transport_usage` for previous day
2. **Calculate KPIs:**
   - Total trips per route
   - Unique vehicles
   - Average delay (seconds)
   - Total delay (seconds)
3. **Load:** Insert into `fact_route_performance`

**Output:** ~180 route-day combinations

---

## 📈 Dashboards & KPIs

### Dashboard 1: Weather Overview
**Purpose:** Monitor environmental conditions  
**Refresh:** Hourly

**Visualizations:**
1. Temperature trends (line chart) - Last 7 days
2. Weather conditions distribution (pie chart)
3. Humidity vs Temperature correlation (scatter plot)
4. Wind speed analysis (bar chart)

---

### Dashboard 2: Real-Time Transport Performance
**Purpose:** Monitor fleet status and route efficiency  
**Refresh:** Every 10 minutes

**Visualizations:**
1. Active Vehicles by Hour (line chart)
2. Top 10 Most Used Routes (bar chart)
3. Average Delay by Route Type (bar chart)
4. Most Active Routes Today (bar chart)
5. Active Vehicles Now (KPI card)

**Key Metrics:**
- Average delay: ~0 minutes (Budapest has efficient transport)
- Active routes: ~180 routes/day
- Peak hours: 7-9 AM, 4-7 PM

---

### Dashboard 3: Transport-Weather Correlation
**Purpose:** Analyze impact of weather on transport  
**Refresh:** Hourly

**Visualizations:**
1. Transport Usage vs Temperature (dual-axis line chart)
2. Most Active Hours of Day (line chart)
3. Daily Transport Trends (multi-line chart)

**Note:** Limited correlation data due to weather dataset size.

---

## ⚠️ Known Limitations

### 1. Stop Key Null Values
**Issue:** `fact_transport_usage.stop_key` is NULL for all records 
**Cause:** BKK Futár API does not provide `stop_id` for vehicles in motion  
**Impact:** Cannot analyze performance by specific bus stops  
**Workaround:** Use route-level aggregations instead

### 2. Limited Weather Data
**Issue:** Only 25 hours of weather data vs 949 hours of transport data  
**Cause:** Weather pipeline started later than transport pipeline  
**Impact:** Limited correlation analysis between weather and delays  
**Solution:** Wait for more historical data to accumulate

### 3. Route Names Empty
**Status:** RESOLVED  
**Fix:** Populated `dim_route.route_name` with `route_id` values

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