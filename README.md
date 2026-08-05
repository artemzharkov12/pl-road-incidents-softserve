# Car Accident ETL Pipeline (SEWIK)

## Project Description
The project is a data processing pipeline (ETL) for road traffic accidents. The process is implemented using Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables). The architecture is built on the Medallion pattern (Bronze, Silver) with physical isolation of database schemas.

## Tech Stack
* Apache Spark (PySpark)
* Databricks Lakeflow (Delta Live Tables)
* Databricks Asset Bundles (DABs)
* Databricks Auto Loader (`cloudFiles`)

## Code Navigation (For Reviewers)
The core logic of the declarative pipelines and orchestration can be found in the following files:
* **Bronze Layer Ingestion:** `src/car_accident_pl_etl/transformations/02_bronze_ingestion.py`
* **Silver Layer Transformations & Expectations:** `src/car_accident_pl_etl/transformations/03_silver_transformations.py`
* **Infrastructure Configuration (Pipelines & Jobs):** `resources/car_accident_pl_etl.pipeline.yml`

## Project Structure
The repository is organized to separate environment setup, raw data landing, and transformation layers.

```text
├── .bundle/
│   └── car_accident_pl/
│       └── dev/
│           ├── artifacts/
│           ├── files/
│           │   ├── .vscode/
│           │   ├── fixtures/
│           │   ├── resources/            # YAML configuration for pipelines and jobs
│           │   ├── src/
│           │   │   ├── car_accident_pl/
│           │   │   └── car_accident_pl_etl/
│           │   │       ├── transformations/
│           │   │       │   ├── 02_bronze_ingestion.py       # Bronze layer logic
│           │   │       │   ├── 03_silver_transformations.py # Silver layer logic
│           │   │       │   └── README.md
│           │   │       └── sample_notebook
│           │   ├── tests/
│           │   ├── .gitignore
│           │   ├── AGENTS.md
│           │   ├── CLAUDE.md
│           │   ├── databricks.yml        # Main bundle configuration file
│           │   └── README.md             # Project documentation
│           └── state/
├── 00_setup/                 # Infrastructure and schema initialization
│   ├── 00a_setup_bronze      # Bronze schema setup
│   └── 00b_setup_silver      # Silver schema setup
├── 01_landing/               
├── 02_bronze/                # Bronze layer ETL logic
│   └── 02a_bronze_car_accident 
├── 03_silver/                # Silver layer ETL logic (isolated workloads)
│   ├── 03a_silver_sewik_accidents
│   ├── 03b_silver_sewik_vehicles
│   └── 03c_silver_sewik_participants
└── README.md                 
```

## Data Architecture
1. **Bronze Layer (`artemzharkov10_bronze.bronze_sewik`)**: 
   Incremental ingestion of raw data (JSON/CSV) from storage (`/Volumes/dbr_dev/artemzharkov10_bronze/raw_data/`) using Auto Loader. Technical metadata (source file path, ingest timestamp) is appended to the data.
   
2. **Silver Layer (`artemzharkov10_silver`)**: 
   Data is read from the Bronze layer, typed, cleansed, and denormalized.
   * `silver_sewik_accidents`: Core accident data with duplicate removal based on the primary key.
   * `silver_sewik_vehicles`: Extracted vehicle data (using the `explode` function).
   * `silver_sewik_participants`: Extracted accident participant data (using the `explode` function).

## Implemented Features (Latest Update)
* **Migration to Declarative Pipelines**: The pipeline is implemented using `@pipelines.table` decorators. 
* **Data Quality Control (Expectations)**: Validation rules are enforced at the Silver layer:
  * Dropping invalid records: `@dp.expect_or_drop("valid_participant_id", "UCZESTNIK_ID IS NOT NULL")`.
  * Halting the pipeline on critical errors: `@dp.expect_or_fail("valid_id", "ID IS NOT NULL")`.
* **Schema Isolation**: To write layers into different schemas (`artemzharkov10_bronze` and `artemzharkov10_silver`), two independent DLT pipelines were created within a single bundle.
* **Orchestration**: A Databricks Job is configured for sequential pipeline execution (the `run_silver` task strictly depends on the successful completion of `run_bronze`).
* **Infrastructure as Code (IaC)**: The deployment of all resources (pipelines and jobs) is fully automated via Databricks Asset Bundles (`resources/*.yml` file).

## Deployment Instructions
Deploying the project requires the Databricks CLI to be installed and a Personal Access Token configured (with permissions to create repositories and manage resources).

```bash
# 1. Authenticate to the Databricks Workspace
databricks configure --host https://<workspace-url>

# 2. Deploy bundle resources (pipelines and jobs)
databricks bundle deploy

# 3. Run the pipeline (Orchestrated via Job)
databricks bundle run car_accident_etl_job


