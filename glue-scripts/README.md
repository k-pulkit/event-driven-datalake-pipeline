# PySpark Glue Scripts

This directory contains the Apache Spark processing scripts executed by AWS Glue in the Silver and Gold pipeline layers.

---

## 📂 Directory Structure

```text
glue-scripts/
├── src/
│   ├── silver_processing.py       # Silver Layer: Normalization, validation, quarantine, Iceberg Merge
│   └── gold_aggregation.py         # Gold Layer: Aggregations, outlier z-score, PostgreSQL JDBC Upsert
├── pyproject.toml                 # Poetry / PEP 621 Python dependency configurations
└── README.md                      # This documentation file
```

---

## ⚙️ Scripts & Arguments Contract

### 1. Silver Processing (`silver_processing.py`)
Standardizes raw ingestion CSV inputs, filters invalid data, routes malformed records to S3 Quarantine, and performs a WAP Iceberg merge into active and history zones.

*   **Runtime Engine:** AWS Glue 5.1 (Apache Spark 3.5.6, Scala 2.12, Python 3.11, Iceberg 1.10.0)
*   **Default Arguments:**
    *   `--catalog_name`: Name of the Spark catalog (e.g. `iceberg_glue`).
    *   `--database_name`: Landing catalog database name.
    *   `--silver_database`: Target Silver catalog database name.
    *   `--silver_table_active`: Active trips target table name (`silver_trips_active`).
    *   `--silver_table_history`: Chronological history target table name (`silver_trips_history`).
    *   `--quarantine_bucket`: Target S3 bucket ID for malformed CSV rows.
    *   `--processed_bucket`: Target S3 bucket ID for Iceberg warehouse storage.
    *   `--s3_file_paths`: JSON-serialized list of S3 CSV file keys to process.
    *   `--iceberg_branch_name`: Name of the Iceberg WAP branch to target.

#### Ingestion Controls
*   **Permissive Reading:** Reads raw CSV records with a Spark schema including a `_corrupt_record` column. If a row cannot parse against the schema, the raw row is isolated and routed to S3 Quarantine.
*   **Lazy Dimensions Lookup:** Loads dimension catalogs (`dim_routes` and `dim_vehicles`) from the landing Glue Catalog using `from_catalog` rather than file-level reads.
*   **Deduplication:** Filters out duplicate input S3 paths, deduplicates the overall batch records before writing to history, and executes window deduplication (`Window.partitionBy("trip_id")`) to prevent cardinality merge violations when updating the active table.

---

### 2. Gold Aggregation (`gold_aggregation.py`)
Computes aggregated metrics and anomaly reports from the Iceberg dataset and loads them into RDS PostgreSQL.

*   **Runtime Engine:** AWS Glue 5.1
*   **Default Arguments:**
    *   `--catalog_name`: Name of the Spark catalog.
    *   `--database_name`: Landing database name.
    *   `--silver_database`: Silver database name.
    *   `--silver_table_active`: Active trips table name.
    *   `--silver_table_history`: History trips table name.
    *   `--iceberg_branch_name`: Name of the Iceberg WAP branch to read.
    *   `--start_date` / `--end_date`: Date boundaries for partition pruning and aggregates recalculation.
    *   `--rds_secret_name`: Friendly name of the Secrets Manager secret storing database credentials.

#### Database Writing Controls
*   **Secrets Manager Retrieval:** Fetches database host, port, credentials, and name dynamically at execution start using `boto3`, avoiding plaintext command arguments.
*   **Transactional Upsert:** 
    1.  Writes aggregates to staging tables (`stage_gold_daily_ridership`, `stage_gold_top_routes_weekly`, `stage_gold_trip_outliers`) using Spark JDBC.
    2.  Instantiates an atomic JVM-level JDBC transaction connection.
    3.  Executes a delete-and-insert statement within date boundaries.
    4.  Commits the transaction upon completion. If any write or query statement fails, performs a full database rollback.
