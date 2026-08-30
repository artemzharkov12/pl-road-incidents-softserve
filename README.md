# DriveRisk Intelligence (DRI)

## Project Description
DriveRisk Intelligence (DRI) is a complete end-to-end data processing pipeline and analytical solution for evaluating road traffic accidents in Poland. The system detects hazardous weather conditions in real-time, classifies them by the level of danger to drivers, and visualizes the risk on an interactive map. 

The architecture strictly follows the Medallion pattern (Bronze, Silver, Gold) with physical isolation of database schemas governed by Databricks Unity Catalog.

### The "Frequency Paradox" & Why We Rejected AI
Using standard Machine Learning for this problem introduces critical logical errors due to highly unbalanced data.
* **Missing Negative Data:** Police databases only record actual incidents, making it impossible to train an AI without fabricating fake "safe" data.
* **The Frequency Paradox:** Warm, dry weather is the most common condition throughout the year, meaning the absolute majority of accidents occur on clear days. An unsupervised AI blindly follows raw volume and falsely concludes that a sunny day is the most dangerous weather.

**The Solution:** DRI uses a custom algorithm to calculate **relative risk**. By dividing the absolute number of accidents by the total historical background hours of a specific weather cluster, the system extracts the true accidents-per-hour rate, proving that events like snowstorms carry a much higher relative risk.

## Architecture Pipelines
The project is split into two core workflows orchestrated via Databricks Asset Bundles (DABs):

* **Historical Data & Risk Calculation (Batch Pipeline):** Ingests historical SEWIK car accident records from GOV.PL and NASA-POWER weather data (2018–2024) across 16 Polish voivodeships. Weather metrics are grouped into simplified clusters (e.g., Frost, HeavyRain), merged with accident data by time and location, and transformed into a Star Schema and a static Normalized Risk Index.
* **Real-Time Hazard Scoring (Streaming Pipeline):** An external producer fetches current conditions from the Open-Weather API every 15 minutes, pushing JSON strings to Azure Event Hubs. Delta Live Tables (DLT) Expectations handle built-in quality control in the Silver layer, routing missing coordinates to a quarantine table to prevent system crashes. Valid streams are instantly joined with the historical weights table in the Gold layer to output live hazard scores.

## Tech Stack
* **Data Engineering:** Apache Spark (PySpark), Databricks Lakeflow (Delta Live Tables), Auto Loader (`cloudFiles`)
* **Data Quality:** Databricks Labs DQX Framework, DLT Expectations
* **Orchestration & CI/CD:** Databricks Asset Bundles (DABs)
* **Governance:** Databricks Unity Catalog (Row-Level Security, Column-Level Security)
* **Analytics & BI:** Databricks AI/BI Dashboards (tracking accident locations, holiday peaks, and demographic stats like 68% male accident rates), Databricks SQL Alerts
* **Infrastructure:** Azure Event Hubs, Azure Databricks

## Project Structure
The repository is organized to maintain a modern DABs architecture, separating source code, configurations, and analytical assets.

```text
├── notebooks/                     
│   ├── 00_setup/                  # Infrastructure and schema initialization
│   ├── 01_bronze/                 
│   ├── 02_silver/                 
│   └── 03_gold/                   
├── Presentation Results/          
│   ├── cache/
│   ├── Dashboards.ipynb           
│   └── Dashboards_history.ipynb   
├── resources/                     # DABs orchestration configurations
│   ├── car_accident_pl_etl.pipeline.yml
│   ├── jobs_config.yml
│   └── realtime_road_hazard_scoring.pipeline.yml
├── sql/                           # Databricks SQL Assets & Governance
│   ├── Alert_lab7.dbquery.ipynb
│   ├── Car accidents 2018-2024 pl.lvdash.json 
│   ├── test_column_level_security.dbquery.ipynb
│   └── test_row_level_security.dbquery.ipynb   
├── src/                           # Core ETL Pipeline Source Code
│   ├── car_accident_pl_etl/
│   │   ├── transformations/       
│   │   ├── README.md
│   │   └── requirements.txt
│   └── realtime_road_hazard_scoring/
│       ├── test/                  # DQX Audits and Pytest files (e.g., test_DQX_framework.py)
│       ├── transformations/       # Streaming Medallion scripts
│       ├── README.md
│       └── requirements.txt
├── .gitignore                
├── databricks.yml                 
├── pyproject.toml                 
└── README.md                      