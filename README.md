# Car Accident ETL Pipeline (SEWIK) & Analytics

## Project Description
The project is a complete end-to-end data processing pipeline (ETL) and analytical solution for road traffic accidents in Poland. The process is implemented using Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables). 

The architecture strictly follows the Medallion pattern (Bronze, Silver, Gold) with physical isolation of database schemas governed by Unity Catalog. Beyond data engineering, the project features a fully functional analytical layer, including a Star-Schema dimensional model, Row-Level/Column-Level Security (RLS/CLS), AI/BI interactive dashboards, AI-powered natural-language querying (Genie Spaces), and automated data quality alerts.

## Tech Stack
* **Data Engineering:** Apache Spark (PySpark), Databricks Lakeflow (Delta Live Tables), Auto Loader (`cloudFiles`)
* **Orchestration & CI/CD:** Databricks Asset Bundles (DABs)
* **Governance:** Databricks Unity Catalog (Grants, Row-Level Security, Column-Level Security)
* **Analytics & BI:** Databricks AI/BI Dashboards, Genie Spaces, Databricks SQL Alerts

## Code Navigation (For Reviewers)
The core logic of the declarative pipelines and orchestration can be found in the following files:
* **Bronze Layer Ingestion:** `src/car_accident_pl_etl/transformations/02_bronze_ingestion.py`
* **Silver Layer (Transformations & Expectations):** `src/car_accident_pl_etl/transformations/03_silver_transformations.py`
* **Gold Layer (Star Schema & Aggregations):** `src/car_accident_pl_etl/transformations/04a_gold_dimentions.py` & `04b_gold_facts.py`
* **Data Governance (RLS/CLS):** `SQL/test_row_level_security.dbquery.ipynb`, `SQL/test_column_level_security.dbquery.ipynb`
* **Infrastructure Configuration (Pipelines & Jobs):** `resources/` directory and `databricks.yml`

## Project Structure
The repository is organized to maintain modern Databricks Asset Bundles (DABs) structure alongside Databricks SQL assets and reference notebooks.

```text
├── notebooks/               
│   ├── 00_setup/             # Infrastructure and schema initialization
│   └── 01_landing/           # Landing zone logic / classical scripts
├── SQL/                      # Databricks SQL Assets (Analytics & Governance)
│   ├── Car accidents 2018-2024 pl.lvdash.json    # AI/BI Dashboard configuration
│   ├── daily_ingestion_volume_trigger.dbalert.json # Data volume monitoring alert
│   ├── test_column_level_security.dbquery.ipynb  # CLS (Data masking) scripts
│   └── test_row_level_security.dbquery.ipynb     # RLS (Data filtering) scripts
├── resources/                
├── src/                      
│   └── car_accident_pl_etl/
│       └── transformations/
│           ├── 02_bronze_ingestion.py
│           ├── 03_silver_transformations.py
│           ├── 04a_gold_dimentions.py
│           └── 04b_gold_facts.py                 
├── .gitignore                
├── databricks.yml            
├── pyproject.toml            
├── README.md   
└── Traffic Accident Analysis   
           