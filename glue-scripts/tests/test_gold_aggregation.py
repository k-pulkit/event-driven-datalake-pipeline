# or ABOUTME: PySpark Unit Tests verifying the Gold aggregation transformation functions in gold_aggregation.py

import pytest
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from src.gold_aggregation import compute_daily_ridership, compute_weekly_top_routes, compute_outliers

@pytest.fixture(scope="session")
def spark_session():
    """Fixture to initialize a local, in-memory Spark session for testing"""
    return SparkSession.builder \
        .master("local[*]") \
        .appName("pyspark-gold-unit-testing") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()


@pytest.fixture(scope="session")
def gold_schema():
    """Fixture returning the schema for pre-joined enriched active trips"""
    return StructType([
        StructField("trip_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("route_id", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("status", StringType(), False),
        StructField("sfn_execution_id", StringType(), False),
        StructField("vehicle_type", StringType(), False),
        StructField("capacity", IntegerType(), False)
    ])


def test_compute_daily_ridership(spark_session, gold_schema):
    """
    Validates daily ridership aggregates (counts, avg capacity, peak hour, durations)
    for a specified date range.
    """
    mock_data = [
        # Route R1, Bus (Capacity 50) - 2 trips at 8 AM, 1 trip at 9 AM on Jan 12 (UTC timezone aware)
        ("T1", "v1", "R1", datetime(2025, 1, 12, 8, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 12, 8, 30, 0, tzinfo=timezone.utc), 10.0, "completed", "exec-1", "bus", 50),
        ("T2", "v2", "R1", datetime(2025, 1, 12, 8, 15, 0, tzinfo=timezone.utc), datetime(2025, 1, 12, 8, 45, 0, tzinfo=timezone.utc), 12.0, "completed", "exec-1", "bus", 50),
        ("T3", "v1", "R1", datetime(2025, 1, 12, 9, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 12, 9, 20, 0, tzinfo=timezone.utc), 8.0, "completed", "exec-1", "bus", 50),
        # Route R2, Train (Capacity 300) - 1 in-progress trip on Jan 12
        ("T4", "v3", "R2", datetime(2025, 1, 12, 12, 0, 0, tzinfo=timezone.utc), None, 0.0, "in_progress", "exec-1", "train", 300),
        # Trip on Jan 13 (should be ignored by start_date/end_date filter)
        ("T5", "v1", "R1", datetime(2025, 1, 13, 10, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 13, 10, 30, 0, tzinfo=timezone.utc), 10.0, "completed", "exec-1", "bus", 50)
    ]

    mock_df = spark_session.createDataFrame(mock_data, schema=gold_schema)
    mock_df.createOrReplaceTempView("active_trips")

    # Run daily ridership aggregate for Jan 12 only
    res_df = compute_daily_ridership(spark_session, "2025-01-12", "2025-01-12")
    results = res_df.collect()

    assert len(results) == 2

    # Check Bus Route R1 metrics
    r1_metrics = next(r for r in results if r["route_id"] == "R1")
    assert r1_metrics["total_trips"] == 3
    assert r1_metrics["unique_vehicles"] == 2
    assert r1_metrics["avg_capacity"] == 50.0
    assert abs(r1_metrics["avg_duration_minutes"] - (30 + 30 + 20) / 3) < 0.01
    assert r1_metrics["peak_hour"] == 8
    assert r1_metrics["trips_completed"] == 3
    assert r1_metrics["trips_in_progress"] == 0

    # Check Train Route R2 metrics
    r2_metrics = next(r for r in results if r["route_id"] == "R2")
    assert r2_metrics["total_trips"] == 1
    assert r2_metrics["trips_in_progress"] == 1
    assert r2_metrics["trips_completed"] == 0


def test_compute_weekly_top_routes(spark_session, gold_schema):
    """
    Validates rolling 7-day lookback calculation boundaries.
    """
    mock_data = [
        # Trip on Jan 5 (outside rolling lookback for Jan 12 calculation date)
        ("T1", "v1", "R1", datetime(2025, 1, 5, 12, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 5, 12, 30, 0, tzinfo=timezone.utc), 10.0, "completed", "exec-1", "bus", 50),
        # Trips on Jan 6 (earliest bound of lookback for Jan 12 calculation date)
        ("T2", "v1", "R1", datetime(2025, 1, 6, 8, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 6, 8, 30, 0, tzinfo=timezone.utc), 10.0, "completed", "exec-1", "bus", 50),
        ("T3", "v2", "R2", datetime(2025, 1, 6, 9, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 6, 9, 30, 0, tzinfo=timezone.utc), 10.0, "completed", "exec-1", "bus", 50),
        # Trips on Jan 12 (latest bound of lookback for Jan 12 calculation date)
        ("T4", "v2", "R2", datetime(2025, 1, 12, 10, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 12, 10, 30, 0, tzinfo=timezone.utc), 10.0, "completed", "exec-1", "bus", 50),
        ("T5", "v3", "R2", datetime(2025, 1, 12, 11, 0, 0, tzinfo=timezone.utc), datetime(2025, 1, 12, 11, 30, 0, tzinfo=timezone.utc), 10.0, "completed", "exec-1", "bus", 50)
    ]

    mock_df = spark_session.createDataFrame(mock_data, schema=gold_schema)
    mock_df.createOrReplaceTempView("active_trips")

    # Run weekly calculation for calculation date Jan 12
    res_df = compute_weekly_top_routes(spark_session, "2025-01-12", "2025-01-12")
    results = res_df.collect()

    assert len(results) == 2

    # Route R2 has 3 trips in Jan 6 - Jan 12 -> Rank 1
    rank_1 = next(r for r in results if r["route_rank"] == 1)
    assert rank_1["route_id"] == "R2"
    assert rank_1["total_trips"] == 3

    # Route R1 has 1 trip in Jan 6 - Jan 12 -> Rank 2 (T1 is excluded!)
    rank_2 = next(r for r in results if r["route_rank"] == 2)
    assert rank_2["route_id"] == "R1"
    assert rank_2["total_trips"] == 1


def test_compute_outliers(spark_session, gold_schema):
    """
    Validates statistical Z-score outliers and telemetry distance outliers.
    Requires at least 10 baseline records to mathematically permit Z-score > 3.0 (Shiffler's Theorem).
    """
    mock_data = []
    
    # 1. Create 15 historical baseline completed trips on Route R1, Bus with duration = 10 mins (mean = 10, stddev = 0)
    for i in range(15):
        mock_data.append((
            f"T_base_{i}", "v1", "R1", 
            datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc), 
            datetime(2025, 1, 10, 8, 10, tzinfo=timezone.utc), 
            5.0, "completed", "exec-old", "bus", 50
        ))
        
    # 2. Add current run execution (exec-current) trips
    # A. Normal trip (duration 10 mins) -> Should be ignored
    mock_data.append((
        "T_norm", "v1", "R1", 
        datetime(2025, 1, 12, 14, 0, tzinfo=timezone.utc), 
        datetime(2025, 1, 12, 14, 10, tzinfo=timezone.utc), 
        5.0, "completed", "exec-current", "bus", 50
    ))
    # B. Statistical duration outlier (duration 120 mins) -> Should be flagged (Z-Score > 3.0)
    # With 15 baseline trips (10m) + 1 normal (10m) + 1 outlier (120m), standard deviation is ~26.4m, mean is ~16.4m
    # Z-score = (120 - 16.4) / 26.4 = ~3.9 (which is > 3.0!)
    mock_data.append((
        "T_outlier_dur", "v1", "R1", 
        datetime(2025, 1, 12, 15, 0, tzinfo=timezone.utc), 
        datetime(2025, 1, 12, 17, 0, tzinfo=timezone.utc), 
        5.0, "completed", "exec-current", "bus", 50
    ))
    # C. Telemetry distance outlier (distance = 95.0 km) -> Should be flagged
    mock_data.append((
        "T_outlier_dist", "v2", "R2", 
        datetime(2025, 1, 12, 16, 0, tzinfo=timezone.utc), 
        datetime(2025, 1, 12, 16, 15, tzinfo=timezone.utc), 
        95.0, "completed", "exec-current", "bus", 50
    ))

    mock_df = spark_session.createDataFrame(mock_data, schema=gold_schema)
    mock_df.createOrReplaceTempView("active_trips")

    # Run outlier detection on Jan 12 for the current run
    res_df = compute_outliers(spark_session, "2025-01-12", "2025-01-12", "exec-current")
    results = res_df.collect()

    # Should return exactly T_outlier_dur and T_outlier_dist
    assert len(results) == 2

    # Check duration outlier T_outlier_dur
    t_dur = next(r for r in results if r["trip_id"] == "T_outlier_dur")
    assert t_dur["outlier_reason"] == "Statistical duration outlier (Z-Score > 3.0)"
    assert t_dur["duration_minutes"] == 120.0

    # Check distance outlier T_outlier_dist
    t_dist = next(r for r in results if r["trip_id"] == "T_outlier_dist")
    assert t_dist["outlier_reason"] == "Abnormal distance outlier (> 80 km)"
