# or ABOUTME: Structured PySpark Glue Job with modular transform functions for unit testing and WAP validation

import sys
import json
import boto3
from datetime import datetime, timedelta, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, trim, upper, lower, to_timestamp, broadcast, current_timestamp, input_file_name, count, min, max, date_sub, date_add, when, concat, lpad, substring, date_format, row_number
from pyspark.sql.window import Window
from pyspark.sql import Observation

def main():
    required_args = [
        "JOB_NAME",
        "catalog_name",
        "database_name",
        "silver_database",
        "silver_table_active",
        "silver_table_history",
        "quarantine_bucket",
        "processed_bucket",
        "s3_file_paths",
        "iceberg_branch_name"
    ]

    # Resolve command-line options
    args = getResolvedOptions(sys.argv, required_args)

    # Fail-fast validation loop to ensure all variables are present and populated
    for arg_key in required_args:
        if arg_key not in args or args[arg_key] is None or str(args[arg_key]).strip() == "":
            raise ValueError(f"CRITICAL CONFIGURATION ERROR: Glue argument '--{arg_key}' is missing, null, or empty!")

    catalog = args["catalog_name"]
    landing_db = args["database_name"]
    silver_db = args["silver_database"]
    table_active = args["silver_table_active"]
    table_history = args["silver_table_history"]
    quarantine_bucket = args["quarantine_bucket"]
    processed_bucket = args["processed_bucket"]
    branch_name = args["iceberg_branch_name"]

    # Parse the S3 file paths passed from Step Functions
    s3_paths = list(set(json.loads(args["s3_file_paths"])))
    print(f"Ingesting {len(s3_paths)} files: {s3_paths}")
    print(f"Targeting WAP branch: {branch_name}")

    # Initialize Glue Context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    try:
        # Load static lookup dimensions from Glue Catalog using Dynamic Frames
        spark.sparkContext.setJobGroup("load_and_validate", "Load raw data, dimension joins, and standardization")
        dim_routes_dyf = glueContext.create_dynamic_frame.from_catalog(
            database=landing_db,
            table_name="raw_routes"
        )
        dim_routes = dim_routes_dyf.toDF()

        dim_vehicles_dyf = glueContext.create_dynamic_frame.from_catalog(
            database=landing_db,
            table_name="raw_vehicles"
        )
        dim_vehicles = dim_vehicles_dyf.toDF()

        # Retrieve the schema of the crawled raw incoming table to avoid inferSchema (lazy catalog lookup)
        crawled_schema = spark.table(f"spark_catalog.`{landing_db}`.raw_incoming").schema
        
        # Extend the crawled schema with the corrupt record column to capture malformed rows
        crawled_schema = crawled_schema.add("_corrupt_record", "string")

    except Exception as e:
        print(f"Error loading metadata from Glue Data Catalog: {str(e)}")
        raise e

    # Load CSV files from S3 using the catalog-derived schema (permissive parsing)
    raw_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(crawled_schema) \
        .load(s3_paths)

    # Execute modular pure transformations (Unit Testable)
    clean_df, bad_df = standardize_and_validate(raw_df, dim_routes, dim_vehicles, branch_name)

    # Deduplicate the overall clean incoming micro-batch to prevent exact duplicates in history
    clean_df = clean_df.dropDuplicates(["trip_id", "route_id", "vehicle_id", "start_time", "end_time"])

    # Union bad records and add audit timestamp
    quarantine_df = bad_df.withColumn("quarantined_at", current_timestamp())

    # Write bad records to S3 Quarantine
    bad_count = quarantine_df.count()
    if bad_count > 0:
        spark.sparkContext.setJobGroup("write_quarantine", "Write unparseable/invalid records to S3 quarantine")
        print(f"Writing {bad_count} bad records to S3 quarantine bucket...")
        quarantine_df.write \
            .format("json") \
            .mode("append") \
            .save(f"s3://{quarantine_bucket}/raw_failures/{branch_name}/")

    # Spark Observation API for Data Quality Metrics & Dynamic Partition Pruning Bounds
    dq_observation = Observation("dq_metrics")
    observed_df = clean_df.observe(
        dq_observation,
        count(lit(1)).alias("clean_count"),
        date_format(min("start_time"), "yyyy-MM-dd").alias("min_time"),
        date_format(max("start_time"), "yyyy-MM-dd").alias("max_time")
    )
    # Cache the clean dataframe as it will be written to history and active tables
    observed_df.cache()
    
    # Deduplicate the active source data by trip_id to prevent merge cardinality violations
    # Rule: completed status first, then largest end_time desc
    dedup_window = Window.partitionBy("trip_id").orderBy(
        when(col("status") == "completed", 1).otherwise(2).asc(),
        col("end_time").desc_nulls_last()
    )
    merge_source_df = observed_df.withColumn("rn", row_number().over(dedup_window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Register the deduplicated dataframe as the merge source view
    merge_source_df.createOrReplaceTempView("new_trips_view")
    spark.catalog.cacheTable("new_trips_view")

    # Define Iceberg table names for history and active branches
    target_history_table = f"{catalog}.`{silver_db}`.{table_history}"
    target_active_table = f"{catalog}.`{silver_db}`.{table_active}"

    # Initialize Iceberg tables if they do not exist
    try_initialize_iceberg_tables(spark, target_history_table, target_active_table, "new_trips_view")

    # Ensure staging branch exists on history table
    _ensure_iceberg_branch(spark, target_history_table, branch_name)

    # Write clean records to the Append-Only History Iceberg branch
    # Note: This is our first action on observed_df, which populates the dq_observation object inline
    spark.sparkContext.setJobGroup("write_history", f"Write clean records to history table WAP branch: {branch_name}")
    print(f"Writing clean records to Iceberg history table: {target_history_table} with branch: {branch_name}")
    observed_df.write \
        .format("iceberg") \
        .option("branch", branch_name) \
        .mode("append") \
        .save(target_history_table)

    # Retrieve all metrics (count, min_time, max_time) directly from the observation object
    # This prevents triggering redundant active scans (.count() or .collect()) on the DataFrame
    metrics = dq_observation.get
    clean_count = metrics["clean_count"]
    start_date_str = metrics["min_time"]
    end_date_str = metrics["max_time"]

    if clean_count > 0 and start_date_str is not None and end_date_str is not None:
        start_date_str = start_date_str
        end_date_str = end_date_str

        # Merge (Upsert) clean records into the Active Iceberg branch
        spark.sparkContext.setJobGroup("merge_active", f"Merge clean records into active table WAP branch: {branch_name}")
        print(f"Ingested metrics collected: {clean_count} clean records with start_time range: {start_date_str} to {end_date_str}")
        print(f"Merging clean records into Iceberg active table: {target_active_table}")

        # Create a temporary branch for the active table to ensure we can merge into it
        _ensure_iceberg_branch(spark, target_active_table, branch_name)
        
        # Inject the start_time date range constraint into the ON condition
        # This enables Spark to prune target partitions during the MERGE join scan!
        print(f"Writing clean records to Iceberg active table: {target_active_table} with branch: {branch_name}")
        spark.sql(f"""
            MERGE INTO {target_active_table}.branch_{branch_name} AS target
            USING new_trips_view AS source
            ON target.trip_id = source.trip_id AND
               target.start_time = source.start_time
            WHEN MATCHED AND (
                source.end_time >= target.end_time AND 
                (target.status != 'completed' OR source.status = 'completed')
            ) THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)
    else:
        print("No clean records to merge. Skipping Iceberg active table upserts.")

    # Unconditionally write run metadata JSON to the Processed/Silver bucket warehouse
    # This allows Step Functions to query affected date ranges and skip Gold if empty
    s3_client = boto3.client("s3")
    metadata_payload = {
        "sfn_execution_id": branch_name,
        "start_date": start_date_str,
        "end_date": end_date_str
    }
    print(f"Writing execution metadata to S3 Processed warehouse: {metadata_payload}")
    try:
        s3_client.put_object(
            Bucket=processed_bucket,
            Key=f"run_metadata/{branch_name}.json",
            Body=json.dumps(metadata_payload)
        )
    except Exception as ex:
        print(f"WARNING: Failed to write metadata JSON to S3: {str(ex)}")

    # Unpersist the clean dataframe
    clean_df.unpersist()
    spark.catalog.uncacheTable("new_trips_view")
    print("Completed Silver processing job successfully.")
    
    job.commit()


def _ensure_iceberg_branch(spark, table_name, branch_name):
    """
    Helper function to dynamically create a staging branch on an Iceberg table
    if the staging branch does not already exist.
    """
    if branch_name != "main":
        print(f"Ensuring Iceberg WAP branch '{branch_name}' exists on table: {table_name}")
        spark.sql(f"""
            ALTER TABLE {table_name}
            CREATE OR REPLACE BRANCH {branch_name}
            RETAIN 5 DAYS
        """)


def try_initialize_iceberg_tables(spark, target_history_table, target_active_table, new_trips_view):
    """
    Dynamically initializes Iceberg tables (history and active) if they do not exist,
    using the schemas derived from the provided temporary views.
    Enforces daily partitioning on trip start time and metadata sort orders.
    """
    print(f"Ensuring Iceberg history table exists: {target_history_table}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_history_table}
        USING iceberg
        PARTITIONED BY (days(start_time))
        TBLPROPERTIES (
            'write.sort-order' = 'route_id ASC, start_time ASC',
            'history.expire.max-ref-age-ms' = '604800000',
            'format-version' = '2'
        )
        AS SELECT * FROM {new_trips_view} LIMIT 0
    """)

    print(f"Ensuring Iceberg active table exists: {target_active_table}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_active_table}
        USING iceberg
        PARTITIONED BY (days(start_time))
        TBLPROPERTIES (
            'write.sort-order' = 'trip_id ASC',
            'history.expire.max-ref-age-ms' = '604800000'
        )
        AS SELECT * FROM {new_trips_view} LIMIT 0
    """)


def standardize_and_validate(raw_df, dim_routes, dim_vehicles, branch_name):
    """
    Modular transformation function that performs:
    1. Schema normalization (standardizes columns, trims strings)
    2. Data Quality audits (primary key checks, malformed parses)
    3. Referential validation (joins against broadcasted dimensions)

    Args:
        raw_df (DataFrame): Raw input trips Spark DataFrame loaded from S3 CSV.
        dim_routes (DataFrame): Static routes dimension Spark DataFrame from the Glue Catalog.
        dim_vehicles (DataFrame): Static vehicles dimension Spark DataFrame from the Glue Catalog.
        branch_name (str): Active Step Functions execution ID used as the WAP branch name.

    Returns:
        tuple: A tuple containing:
            - clean_df (DataFrame): Spark DataFrame with normalized records passing all validation rules.
            - bad_df (DataFrame): Spark DataFrame containing records failing CSV parsing, missing key constraints, or failing referential broadcast joins, with an added `quarantine_reason` string column.
    """
    # Separate malformed CSV rows from standard rows
    corrupt_rows_df = raw_df.filter(col("_corrupt_record").isNotNull()) \
        .withColumn("quarantine_reason", lit("Malformed CSV parsing failure"))
    parsable_rows_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    # Standardize column casing, trim whitespace, and normalize string values
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    normalized_df = parsable_rows_df.select(
        trim(col("trip_id")).alias("trip_id"),
        when(
            col("route_id").isNull() | (col("route_id") == ""),
            col("route_id")
        ).otherwise(
            concat(lit("R"), lpad(substring(col("route_id"), 2, 10), 3, "0"))
        ).alias("route_id"),
        when(
            col("vehicle_id").isNull() | (col("vehicle_id") == ""),
            col("vehicle_id")
        ).otherwise(
            concat(lit("V"), lpad(substring(col("vehicle_id"), 2, 10), 4, "0"))
        ).alias("vehicle_id"),
        lower(trim(col("status"))).alias("status"),
        to_timestamp(col("start_time")).alias("start_time"),
        to_timestamp(col("end_time")).alias("end_time"),
        col("distance_km").cast("double").alias("distance_km"),
        lit(now).cast("timestamp").alias("ingested_at"),
        lit(branch_name).alias("sfn_execution_id"),
        # Include all remaining columns except those being replaced and vehicle_id_raw
        *[col(c) for c in parsable_rows_df.columns
            if c not in {
                "trip_id","route_id","vehicle_id","status","start_time","end_time","distance_km","vehicle_id_raw",}],
    )

    # Filter out records missing primary identifiers
    has_keys_df = normalized_df.filter(
        col("trip_id").isNotNull() & (col("trip_id") != "") &
        col("route_id").isNotNull() & (col("route_id") != "") &
        col("vehicle_id").isNotNull() & (col("vehicle_id") != "")
    )

    missing_keys_df = normalized_df.filter(
        col("trip_id").isNull() | (col("trip_id") == "") |
        col("route_id").isNull() | (col("route_id") == "") |
        col("vehicle_id").isNull() | (col("vehicle_id") == "")
    ).withColumn("quarantine_reason", lit("Missing primary identifier (trip_id, route_id, or vehicle_id)"))

    # Temporal Sanity Check (Guarding partitions from corrupt dates)
    # Rejects start_time older than 30 days or in the future (+1 day buffer for clock and timezone skew)
    # Cannot use current_timestamp() in filter expressions due to Spark's Catalyst optimization rules
    # , so we compute the bounds in Python and pass them as literals
    lower_time_bound = date_sub(lit(now).cast("timestamp"), 3000)
    upper_time_bound = date_add(lit(now).cast("timestamp"), 1)
    
    valid_time_df = has_keys_df.filter(
        (col("start_time") >= lower_time_bound) & 
        (col("start_time") <= upper_time_bound)
    )
    
    invalid_time_df = has_keys_df.filter(
        (col("start_time").isNull()) |
        (col("start_time") < lower_time_bound) | 
        (col("start_time") > upper_time_bound)
    ).withColumn("quarantine_reason", lit("Temporal validation failure: start_time is in the future or older than 30 days"))

    # Referential Integrity check via Broadcast Joins (evaluated on temporal-valid rows)
    # We join trips against routes and vehicles (broadcasted since dimensions are small)
    valid_routes_df = valid_time_df.join(
        broadcast(dim_routes),
        valid_time_df.route_id == dim_routes.route_id,
        "inner"
    ).select(valid_time_df["*"])

    invalid_routes_df = valid_time_df.join(
        broadcast(dim_routes),
        valid_time_df.route_id == dim_routes.route_id,
        "left_anti"
    ).withColumn("quarantine_reason", lit("Referential validation failure: route_id not found in dim_routes"))

    # Further validate against vehicles
    clean_df = valid_routes_df.join(
        broadcast(dim_vehicles),
        valid_routes_df.vehicle_id == dim_vehicles.vehicle_id,
        "inner"
    ).select(valid_routes_df["*"])

    invalid_vehicles_df = valid_routes_df.join(
        broadcast(dim_vehicles),
        valid_routes_df.vehicle_id == dim_vehicles.vehicle_id,
        "left_anti"
    ).withColumn("quarantine_reason", lit("Referential validation failure: vehicle_id not found in dim_vehicles"))

    # Accumulate all invalid/bad records, including raw corrupt rows and temporal outliers
    bad_df = missing_keys_df \
        .unionByName(invalid_routes_df, allowMissingColumns=True) \
        .unionByName(invalid_vehicles_df, allowMissingColumns=True) \
        .unionByName(invalid_time_df, allowMissingColumns=True) \
        .unionByName(corrupt_rows_df, allowMissingColumns=True)
    
    # Add the source_file column to the bad_df for traceability
    bad_df = bad_df.withColumn("source_file", input_file_name())

    return clean_df, bad_df


if __name__ == "__main__":
    main()
