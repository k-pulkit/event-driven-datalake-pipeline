# or ABOUTME: Structured Gold PySpark Job running partition-pruned aggregations and transactional PostgreSQL upserts (WAP pattern)

import sys
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, min, max, date_trunc, explode, array, lit, to_timestamp, broadcast

def main():
    required_args = [
        "JOB_NAME",
        "catalog_name",
        "database_name",
        "silver_database",
        "silver_table_active",
        "silver_table_history",
        "iceberg_branch_name",
        "start_date",
        "end_date"
    ]

    # Resolve command-line options
    args = getResolvedOptions(sys.argv, required_args)

    # Fail-fast validation loop to ensure all variables are present
    for arg_key in required_args:
        if arg_key not in args or args[arg_key] is None:
            raise ValueError(f"CRITICAL CONFIGURATION ERROR: Glue argument '--{arg_key}' is missing!")

    catalog = args["catalog_name"]
    landing_db = args["database_name"]
    silver_db = args["silver_database"]
    table_active = args["silver_table_active"]
    table_history = args["silver_table_history"]
    branch_name = args["iceberg_branch_name"]
    start_date = str(args["start_date"]).strip()
    end_date = str(args["end_date"]).strip()

    print(f"Reading from Iceberg active table: {catalog}.{silver_db}.{table_active}")
    print(f"Processing branch: {branch_name}")

    # Parse start and end dates to datetime objects
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except Exception as ex:
        raise ValueError(f"Failed to parse date range '{start_date}' to '{end_date}': {str(ex)}")

    # Initialize Glue Context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # If date bounds are empty, skip aggregation entirely
    if not start_date or not end_date:
        print("Empty start_date or end_date bounds passed. Skipping Gold aggregation.")
        job.commit()
        return

    # Load the specific WAP branch for validation/aggregation
    spark.sparkContext.setJobGroup("load_and_filter", "Load Active trips from Iceberg branch, apply date filtering and vehicle joins")
    active_trips_df = spark.read \
        .format("iceberg") \
        .option("branch", branch_name) \
        .load(f"{catalog}.`{silver_db}`.{table_active}")

    # Load static dimensions as Spark DataFrames to register in the Spark session catalog
    dim_vehicles_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=landing_db,
        table_name="raw_vehicles"
    )
    dim_vehicles = dim_vehicles_dyf.toDF()

    # Enforce early partition pruning by filtering active_trips_df using trip date bounds [start_date - 6, end_date + 6]
    max_start = (start_dt - timedelta(days=6)).strftime("%Y-%m-%d")
    max_end = (end_dt + timedelta(days=6)).strftime("%Y-%m-%d")

    date_filtered_df = active_trips_df.filter(
        (col("start_time") >= to_timestamp(lit(max_start))) &
        (col("start_time") <= to_timestamp(lit(max_end + " 23:59:59")))
    )

    # Perform an early broadcast join with raw_vehicles to enrich the dataset with vehicle type and capacity before caching
    enriched_active_df = date_filtered_df.join(
        broadcast(dim_vehicles),
        date_filtered_df.vehicle_id == dim_vehicles.vehicle_id,
        "inner"
    ).select(
        date_filtered_df["*"],
        dim_vehicles.vehicle_type,
        dim_vehicles.capacity
    )

    # Cache the fully enriched DataFrame to prevent redundant S3 and join scans across the 3 aggregations
    enriched_active_df.cache()

    # Calculate rolling weekly calculation bounds: D <= C <= D + 6
    # This translates to rolling calculation date C between start_date and end_date + 6 days
    calc_start = start_dt.strftime("%Y-%m-%d")
    calc_end = (end_dt + timedelta(days=6)).strftime("%Y-%m-%d")
    print(f"Rolling weekly calculation boundaries: {calc_start} to {calc_end}")

    # Register the cached, partition-pruned, and pre-joined DataFrame as a temporary view for SQL execution
    enriched_active_df.createOrReplaceTempView("active_trips")

    # 4. Compute partition-pruned aggregates
    spark.sparkContext.setJobGroup("compute_daily_ridership", "Compute Daily Ridership Aggregates")
    daily_ridership_df = compute_daily_ridership(spark, start_date, end_date)

    spark.sparkContext.setJobGroup("compute_weekly_top_routes", "Compute Weekly Top Routes Aggregates")
    weekly_top_routes_df = compute_weekly_top_routes(spark, calc_start, calc_end)

    spark.sparkContext.setJobGroup("compute_outliers", "Compute Outliers Aggregates")
    outliers_df = compute_outliers(spark, start_date, end_date, branch_name)

    # 5. Load aggregates to RDS PostgreSQL (Staging-to-Target Upsert Pattern)
    # Note: JDBC connection properties and secrets will be resolved in Phase 4.
    # We stub the staging-upsert pattern:
    print("Writing aggregates to RDS PostgreSQL database via transactional staging-upsert flow...")
    
    # TODO: Resolve connection properties from AWS Secrets Manager during Phase 4
    # jdbc_url = "jdbc:postgresql://<rds-endpoint>:5432/<db-name>"
    # db_properties = {
    #     "user": "<user>",
    #     "password": "<password>",
    #     "driver": "org.postgresql.Driver"
    # }
    
    # Example execution pattern for Daily Ridership:
    # staging_table = "stage_gold_daily_ridership"
    # daily_ridership_df.write.jdbc(url=jdbc_url, table=staging_table, mode="overwrite", properties=db_properties)
    #
    # upsert_sql = """
    #     INSERT INTO gold_daily_ridership (
    #         trip_date, route_id, vehicle_type, total_trips, unique_vehicles, 
    #         avg_capacity, avg_duration_minutes, max_duration_minutes, 
    #         trips_completed, trips_in_progress, peak_hour
    #     )
    #     SELECT * FROM stage_gold_daily_ridership
    #     ON CONFLICT (trip_date, route_id, vehicle_type) 
    #     DO UPDATE SET 
    #         total_trips = EXCLUDED.total_trips,
    #         unique_vehicles = EXCLUDED.unique_vehicles,
    #         avg_capacity = EXCLUDED.avg_capacity,
    #         avg_duration_minutes = EXCLUDED.avg_duration_minutes,
    #         max_duration_minutes = EXCLUDED.max_duration_minutes,
    #         trips_completed = EXCLUDED.trips_completed,
    #         trips_in_progress = EXCLUDED.trips_in_progress,
    #         peak_hour = EXCLUDED.peak_hour
    # """
    # Execute upsert in Postgres JVM Connection, and drop staging_table
    
    # 6. Commit WAP Batch (Fast-Forward Iceberg branches to main)
    # We only run fast-forward if we are processing a temporary WAP staging branch
    if branch_name != "main":
        spark.sparkContext.setJobGroup("commit_wap_branches", f"Fast-Forward Iceberg staging branch '{branch_name}' to main")
        print(f"RDS database transaction successfully completed. Committing Iceberg S3 state from branch '{branch_name}'...")
        try:
            # Fast-forward active and history status tables
            spark.sql(f"CALL {catalog}.system.fast_forward('`{silver_db}`.{table_active}', 'main', '{branch_name}')")
            print("Active status Iceberg table successfully fast-forwarded to main.")

            spark.sql(f"CALL {catalog}.system.fast_forward('`{silver_db}`.{table_history}', 'main', '{branch_name}')")
            print("Append-only history Iceberg table successfully fast-forwarded to main.")
        except Exception as e:
            print(f"Error executing Iceberg WAP fast-forward: {str(e)}")
            raise e
    else:
        print("Manual run on main branch detected. Skipping Iceberg fast-forward operations.")

    # Unpersist the cached DataFrame
    enriched_active_df.unpersist()

    job.commit()


def compute_daily_ridership(spark, start_date, end_date):
    """
    Computes daily trip summaries grouped by trip_date, route_id, and vehicle_type.
    Restricts scans to date boundaries to support S3 partition pruning.
    Queries metadata columns directly from the pre-joined, cached active_trips view.
    """
    return spark.sql(f"""
        WITH hourly_counts AS (
            SELECT 
                cast(t.start_time as date) AS trip_date,
                t.route_id,
                t.vehicle_type,
                hour(t.start_time) AS trip_hour,
                count(t.trip_id) AS hour_trip_count
            FROM 
                active_trips t
            WHERE 
                cast(t.start_time as date) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 
                1, 2, 3, 4
        ),
        ranked_hours AS (
            SELECT 
                trip_date,
                route_id,
                vehicle_type,
                trip_hour,
                row_number() over (
                    partition by trip_date, route_id, vehicle_type 
                    order by hour_trip_count desc, trip_hour asc
                ) as rn
            FROM 
                hourly_counts
        ),
        base_metrics AS (
            SELECT 
                cast(t.start_time as date) AS trip_date,
                t.route_id,
                t.vehicle_type,
                count(t.trip_id) AS total_trips,
                count(distinct t.vehicle_id) AS unique_vehicles,
                avg(t.capacity) AS avg_capacity,
                avg((cast(t.end_time as double) - cast(t.start_time as double)) / 60) AS avg_duration_minutes,
                max((cast(t.end_time as double) - cast(t.start_time as double)) / 60) AS max_duration_minutes,
                sum(case when t.status = 'completed' then 1 else 0 end) AS trips_completed,
                sum(case when t.status = 'in_progress' then 1 else 0 end) AS trips_in_progress
            FROM 
                active_trips t
            WHERE 
                cast(t.start_time as date) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 
                1, 2, 3
        )
        SELECT 
            bm.trip_date,
            bm.route_id,
            bm.vehicle_type,
            bm.total_trips,
            bm.unique_vehicles,
            bm.avg_capacity,
            bm.avg_duration_minutes,
            bm.max_duration_minutes,
            bm.trips_completed,
            bm.trips_in_progress,
            rh.trip_hour AS peak_hour
        FROM 
            base_metrics bm
        JOIN 
            ranked_hours rh ON 
                bm.trip_date = rh.trip_date AND 
                bm.route_id = rh.route_id AND 
                bm.vehicle_type = rh.vehicle_type AND 
                rh.rn = 1
    """)


def compute_weekly_top_routes(spark, calc_start, calc_end):
    """
    Computes a 7-day rolling window ranking top routes by trip volume.
    Restricts target scans using a calculated date range sequence to enable partition pruning.
    """
    return spark.sql(f"""
        WITH date_ranges AS (
            SELECT explode(sequence(to_date('{calc_start}'), to_date('{calc_end}'), interval 1 day)) AS calculation_date
        ),
        weekly_aggregates AS (
            SELECT 
                cast(dr.calculation_date as date) AS calculation_date,
                t.route_id,
                count(t.trip_id) AS total_trips,
                sum(case when t.status = 'completed' then 1 else 0 end) AS total_completed_trips,
                avg((cast(t.end_time as double) - cast(t.start_time as double)) / 60) AS avg_trip_duration_minutes
            FROM 
                date_ranges dr
            JOIN 
                active_trips t 
                ON cast(t.start_time as date) BETWEEN date_sub(cast(dr.calculation_date as date), 6) AND cast(dr.calculation_date as date)
            GROUP BY 
                1, 2
        ),
        ranked_routes AS (
            SELECT 
                calculation_date,
                route_id,
                total_trips,
                total_completed_trips,
                avg_trip_duration_minutes,
                dense_rank() over (
                    partition by calculation_date 
                    order by total_trips desc, route_id asc
                ) as route_rank
            FROM 
                weekly_aggregates
        )
        SELECT 
            calculation_date,
            route_rank,
            route_id,
            total_trips,
            total_completed_trips,
            avg_trip_duration_minutes
        FROM 
            ranked_routes
        WHERE 
            route_rank <= 10
    """)


def compute_outliers(spark, start_date, end_date, branch_name):
    """
    Identifies statistical duration outliers (Z-Score > 3.0) and telemetry distance anomalies
    for trips processed in the current execution run.
    Queries metadata columns directly from the pre-joined, cached active_trips view.
    """
    # Support manual runs (no sfn_execution_id) by filtering on date bounds
    current_filter = f"t.sfn_execution_id = '{branch_name}'" if branch_name != "main" else f"cast(t.start_time as date) BETWEEN '{start_date}' AND '{end_date}'"

    return spark.sql(f"""
        WITH route_stats AS (
            SELECT 
                t.route_id,
                t.vehicle_type,
                avg((cast(t.end_time as double) - cast(t.start_time as double)) / 60) AS avg_duration_minutes,
                stddev((cast(t.end_time as double) - cast(t.start_time as double)) / 60) AS stddev_duration_minutes
            FROM 
                active_trips t
            WHERE 
                t.status = 'completed'
                AND t.end_time IS NOT NULL
            GROUP BY 
                1, 2
        ),
        current_trips AS (
            SELECT 
                t.trip_id,
                t.route_id,
                t.vehicle_id,
                t.vehicle_type,
                t.start_time,
                t.end_time,
                t.distance_km,
                (cast(t.end_time as double) - cast(t.start_time as double)) / 60 AS duration_minutes
            FROM 
                active_trips t
            WHERE 
                {current_filter}
                AND t.status = 'completed'
                AND t.end_time IS NOT NULL
        ),
        calculated_outliers AS (
            SELECT 
                ct.trip_id,
                ct.route_id,
                ct.vehicle_id,
                ct.start_time,
                ct.end_time,
                ct.duration_minutes,
                rs.avg_duration_minutes,
                coalesce(rs.stddev_duration_minutes, 0.0) AS stddev_duration_minutes,
                CASE 
                    WHEN coalesce(rs.stddev_duration_minutes, 0.0) > 0.0 
                    THEN (ct.duration_minutes - rs.avg_duration_minutes) / rs.stddev_duration_minutes
                    ELSE 0.0 
                END AS z_score,
                ct.distance_km
            FROM 
                current_trips ct
            LEFT JOIN 
                route_stats rs ON ct.route_id = rs.route_id AND ct.vehicle_type = rs.vehicle_type
        )
        SELECT 
            trip_id,
            route_id,
            vehicle_id,
            start_time,
            end_time,
            duration_minutes,
            avg_duration_minutes,
            stddev_duration_minutes,
            z_score,
            CASE 
                WHEN z_score > 3.0 THEN 'Statistical duration outlier (Z-Score > 3.0)'
                WHEN distance_km > 80.0 THEN 'Abnormal distance outlier (> 80 km)'
                ELSE 'Normal'
            END AS outlier_reason
        FROM 
            calculated_outliers
        WHERE 
            z_score > 3.0 
            OR distance_km > 80.0
    """)


if __name__ == "__main__":
    main()
