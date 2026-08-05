# Car Accident ETL Pipeline (SEWIK)

## Project Description
The project is a data processing pipeline (ETL) for road traffic accidents. The process is implemented using Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables), alongside a classic Structured Streaming reference approach. The architecture is built on the Medallion pattern (Bronze, Silver) with physical isolation of database schemas.

## Tech Stack
* Apache Spark (PySpark & Structured Streaming)
* Databricks Lakeflow (Delta Live Tables)
* Databricks Asset Bundles (DABs)
* Databricks Auto Loader (`cloudFiles`)

## Code Navigation (For Reviewers)
The core logic of the declarative pipelines and orchestration can be found in the following files:
* **Bronze Layer Ingestion:** `src/car_accident_pl_etl/transformations/02_bronze_ingestion.py`
* **Silver Layer Transformations & Expectations:** `src/car_accident_pl_etl/transformations/03_silver_transformations.py`
* **Infrastructure Configuration (Pipelines & Jobs):** `resources/car_accident_pl_etl.pipeline.yml`

## Project Structure
The repository is organized to maintain both the classic reference notebooks and the modern Databricks Asset Bundles (DABs) structure.

```text
├── 00_setup/                 # Infrastructure and schema initialization
│   ├── 00a_setup_bronze      # Bronze schema setup
│   └── 00b_setup_silver      # Silver schema setup
├── 01_landing/               
├── 02_bronze/                # Classic Bronze layer ETL logic
│   └── 02a_bronze_car_accident 
├── 03_silver/                # Classic Silver layer ETL logic (isolated workloads)
│   ├── 03a_silver_sewik_accidents
│   ├── 03b_silver_sewik_vehicles
│   └── 03c_silver_sewik_participants
├── resources/                # YAML configuration for pipelines and jobs (DABs)
├── src/                      # Source code for declarative pipelines
│   └── car_accident_pl_etl/
│       └── transformations/
│           ├── 02_bronze_ingestion.py
│           └── 03_silver_transformations.py
├── tests/                    
├── .gitignore                # Excludes .bundle/ and temporary build files
├── databricks.yml            # Main bundle configuration file
├── pyproject.toml           
└── README.md                
