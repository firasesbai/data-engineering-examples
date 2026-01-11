
# Open Source Data Platform Template

This directory provides a **reusable infrastructure template** for setting up a Modern Data Stack locally using Docker. 
It is designed to be a starting point for your own data engineering projects, allowing you to easily spin up a complete platform with best-in-class open-source tools.

> **Note**: This project includes an end-to-end example pipeline using **NYC Taxi data**. This pipeline serves only as a **demo** to verify that all components (Airflow, MinIO, DuckDB, dbt, Superset) are communicating correctly. You should replace this with your own logic.

## 🏗 Architecture

The platform follows a **Medallion Architecture** pattern (Bronze -> Silver) using the following open-source technologies:

| Component | Technology | Role |
|-----------|------------|------|
| **Data Lake** | **MinIO** | S3-compatible Object Storage. Stores raw CSVs and intermediate data. |
| **Compute / OLAP** | **DuckDB** | In-process SQL OLAP engine. Handles high-performance queries and transformations. |
| **Orchestration** | **Airflow** | Workflow engine. Manages the ETL pipeline (`Extract` -> `dbt Build`). |
| **Transformations** | **dbt** | Defines data models and tests. Runs inside Airflow using `dbt-duckdb`. |
| **Metadata / OLTP** | **Postgres** | Stores Airflow/Superset metadata. Uses `pgvector` image for future AI extensibility. |
| **Visualization** | **Superset** | BI Tool. Connects to DuckDB to visualize the data lake. |

### Data Flow
1.  **Extract**: Airflow DAG checks for/creates a bucket in MinIO and uploads local sample data (`data/nyc_taxi_sample.csv`) to `s3://data-lake/landing/`.
2.  **Transform**: Airflow triggers `dbt build` which runs:
    -   **Bronze (`stg_nyc_taxi`)**: A View directly over the S3 CSV files.
    -   **Silver (`mart_daily_revenue`)**: A Table materialized in the local DuckDB database (`data/my_db.duckdb`) with daily aggregations.
3.  **Serve**: Superset connects to the persisted DuckDB file and queries the `mart_daily_revenue` table.

## 🚀 Quick Start

### 1. Prerequisites
- Docker and Docker Compose

### 2. Start the Platform
```bash
cd oss-data-platform
# Start all services (builds custom images first)
docker compose up -d --build
```

### 3. Run the Pipeline (Airflow)
1.  Navigate to [http://localhost:8080](http://localhost:8080)
    -   **User**: `airflow`
    -   **Password**: `airflow`
2.  Find the `nyc_taxi_etl` DAG.
3.  **Unpause** the DAG (toggle the switch on the left).
4.  Trigger it manually (Play button) if needed.
5.  Wait for the `dbt_run` task to complete (green).

### 4. Visualize Data (Superset)
1.  Navigate to [http://localhost:8088](http://localhost:8088)
    -   **User**: `admin`
    -   **Password**: `admin`
2.  Go to **Settings (Top Right) -> Database Connections**.
3.  Click **+ Database**.
4.  Select **DuckDB** (or "Other" -> Type `duckdb`).
5.  Enter the **SQLAlchemy URI**:
    ```text
    duckdb:////app/data/my_db.duckdb
    ```
    *(Note the 4 slashes: `sqlite:///` protocol + `/absolute/path`)*.
6.  Click **Test Connection** (should say "Connection looks good!"), then **Connect**.
7.  Go to **SQL Lab**, select the **DuckDB** database, and run:
    ```sql
    SELECT * FROM mart_daily_revenue;
    ```

## 🛠 Advanced / Ad-Hoc Queries
You can also query MinIO files directly from Superset without using the pre-built Marts (Data Lakehouse style).

1.  Create a **new** Database connection in Superset:
    -   **Display Name**: `Ad-Hoc Data Lake`
    -   **SQLAlchemy URI**: `duckdb:///:memory:`
2.  **IMPORTANT**: Go to the **Advanced** -> **SQL Lab** tab and check **Allow DML**. (Required for `INSTALL/LOAD` commands).
3.  In SQL Lab, run:
    ```sql
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';

    -- Query raw data directly from S3
    SELECT * FROM read_csv_auto('s3://data-lake/landing/nyc_taxi_sample.csv', header=True);
    ```

## 📂 Directory Structure
- `airflow/`: Custom Airflow Docker image configuration.
- `dags/`: Airflow DAGs (Workflows) definition.
- `data/`: Local data volume (stores sample data and the DuckDB database file).
- `dbt_project/`: dbt project containing data models and transformation logic.
- `postgres/`: Database initialization scripts (e.g., `init.sql`).
- `superset/`: Custom Superset Docker image configuration.
- `docker-compose.yml`: Entrypoint for all services.
