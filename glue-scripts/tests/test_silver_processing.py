# or ABOUTME: PySpark Unit Tests verifying the standardize_and_validate transformation logic in silver_processing.py

import pytest
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col
from src.silver_processing import standardize_and_validate

@pytest.fixture(scope="session")
def spark_session():
    """Fixture to initialize a local, in-memory Spark session for testing"""
    return SparkSession.builder \
        .master("local[*]") \
        .appName("pyspark-unit-testing") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()


@pytest.fixture(scope="session")
def test_schemas():
    """Fixture returning a dictionary of schemas for mock DataFrames"""
    route_schema = StructType([
        StructField("route_id", StringType(), True)
    ])

    vehicle_schema = StructType([
        StructField("vehicle_id", StringType(), True)
    ])

    trips_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("vehicle_id_raw", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])

    return {
        "routes": route_schema,
        "vehicles": vehicle_schema,
        "trips": trips_schema
    }


def test_standardize_and_validate_success(spark_session, test_schemas):
    """
    Test Case 1: Standard ingestion of a valid record.
    Verifies that a clean record passing all referential and constraint audits
    is routed to clean_df, while standardizing string fields.
    """
    # Create mock route and vehicle dimension DataFrames using fixture schemas
    mock_routes = spark_session.createDataFrame([("R001",), ("R002",)], schema=test_schemas["routes"])
    mock_vehicles = spark_session.createDataFrame([("V0001",), ("V0002",)], schema=test_schemas["vehicles"])

    # Define a valid current timestamp for the trip start/end times
    valid_now = datetime.now(timezone.utc)

    mock_raw_trips = spark_session.createDataFrame([
        # Clean record: trip_id is present, vehicle & route match dimensions, start/end dates are valid, with raw duplicate
        ("1438575a", "v001", "R01", valid_now, valid_now, 17.68, "completed", "V001", "s3://landing/trips.csv", None)
    ], schema=test_schemas["trips"])

    # Execute transformations
    clean_df, bad_df = standardize_and_validate(mock_raw_trips, mock_routes, mock_vehicles, "test_sfn_execution")

    # Assertions
    assert clean_df.count() == 1
    assert bad_df.count() == 0

    # Verify normalization (status converted to lowercase, duplicate dropped, route and vehicle padded)
    normalized_row = clean_df.collect()[0]
    assert "vehicle_id_raw" not in clean_df.columns
    assert normalized_row["status"] == "completed"
    assert normalized_row["route_id"] == "R001"
    assert normalized_row["vehicle_id"] == "V0001"
    assert normalized_row["sfn_execution_id"] == "test_sfn_execution"


def test_standardize_and_validate_missing_keys(spark_session, test_schemas):
    """
    Test Case 2: Missing key validation.
    Verifies that records missing critical primary keys (trip_id, route_id, or vehicle_id)
    are caught and routed to bad_df with the appropriate quarantine reason.
    """
    mock_routes = spark_session.createDataFrame([("R001",)], schema=test_schemas["routes"])
    mock_vehicles = spark_session.createDataFrame([("V0001",)], schema=test_schemas["vehicles"])

    valid_now = datetime.now(timezone.utc)

    mock_raw_trips = spark_session.createDataFrame([
        # Missing trip_id (with valid dates)
        (None, "v001", "R01", valid_now, valid_now, 10.0, "completed", "V001", "s3://landing/trips.csv", None),
        # Empty route_id (with valid dates)
        ("1438575b", "v001", "", valid_now, valid_now, 10.0, "completed", "V001", "s3://landing/trips.csv", None)
    ], schema=test_schemas["trips"])

    clean_df, bad_df = standardize_and_validate(mock_raw_trips, mock_routes, mock_vehicles, "test_sfn_execution")

    assert clean_df.count() == 0
    assert bad_df.count() == 2

    # Assert reasons
    reasons = [row["quarantine_reason"] for row in bad_df.collect()]
    for reason in reasons:
        assert "Missing primary identifier" in reason


def test_standardize_and_validate_referential_integrity(spark_session, test_schemas):
    """
    Test Case 3: Referential integrity checks.
    Verifies that records with route_id or vehicle_id values missing from the dimension tables
    are correctly partitioned to bad_df.
    """
    mock_routes = spark_session.createDataFrame([("R001",)], schema=test_schemas["routes"])
    mock_vehicles = spark_session.createDataFrame([("V0001",)], schema=test_schemas["vehicles"])

    valid_now = datetime.now(timezone.utc)

    mock_raw_trips = spark_session.createDataFrame([
        # Route ID missing from dimensions
        ("T1", "v001", "R99", valid_now, valid_now, 15.0, "completed", "V001", "s3://landing/trips.csv", None),
        # Vehicle ID missing from dimensions
        ("T2", "v999", "R01", valid_now, valid_now, 15.0, "completed", "V999", "s3://landing/trips.csv", None)
    ], schema=test_schemas["trips"])

    clean_df, bad_df = standardize_and_validate(mock_raw_trips, mock_routes, mock_vehicles, "test_sfn_execution")

    assert clean_df.count() == 0
    assert bad_df.count() == 2

    # Assert specific reasons are set
    bad_rows = bad_df.collect()
    route_fail = [r for r in bad_rows if r["trip_id"] == "T1"][0]
    vehicle_fail = [r for r in bad_rows if r["trip_id"] == "T2"][0]

    assert "route_id not found in dim_routes" in route_fail["quarantine_reason"]
    assert "vehicle_id not found in dim_vehicles" in vehicle_fail["quarantine_reason"]


def test_standardize_and_validate_corrupt_record(spark_session, test_schemas):
    """
    Test Case 4: Malformed CSV parsing (corrupt record capture).
    Verifies that rows flagged by Spark's permissive CSV reader as malformed (unparseable lines)
    are separated and routed to bad_df.
    """
    mock_routes = spark_session.createDataFrame([("R001",)], schema=test_schemas["routes"])
    mock_vehicles = spark_session.createDataFrame([("V0001",)], schema=test_schemas["vehicles"])

    mock_raw_trips = spark_session.createDataFrame([
        # Row has corrupt CSV data
        ("T1", "v001", "R01", None, None, None, None, None, "s3://landing/trips.csv", "T1,v001,R01,malformed,data,extra_col")
    ], schema=test_schemas["trips"])

    clean_df, bad_df = standardize_and_validate(mock_raw_trips, mock_routes, mock_vehicles, "test_sfn_execution")

    assert clean_df.count() == 0
    assert bad_df.count() == 1

    corrupt_row = bad_df.collect()[0]
    assert corrupt_row["quarantine_reason"] == "Malformed CSV parsing failure"
    assert corrupt_row["_corrupt_record"] == "T1,v001,R01,malformed,data,extra_col"


def test_standardize_and_validate_temporal_sanity(spark_session, test_schemas):
    """
    Test Case 5: Temporal sanity validation (out-of-bounds start_time).
    Verifies that records with start_time values unreasonably in the past (>30 days)
    or in the future are isolated and routed to bad_df.
    """
    mock_routes = spark_session.createDataFrame([("R001",)], schema=test_schemas["routes"])
    mock_vehicles = spark_session.createDataFrame([("V0001",)], schema=test_schemas["vehicles"])

    past_date = datetime.now(timezone.utc) - timedelta(days=45)
    future_date = datetime.now(timezone.utc) + timedelta(days=5)

    mock_raw_trips = spark_session.createDataFrame([
        # Start time too far in the past (45 days ago)
        ("T1", "v001", "R01", past_date, None, 10.0, "completed", "V001", "s3://landing/trips.csv", None),
        # Start time in the future (5 days ahead)
        ("T2", "v001", "R01", future_date, None, 10.0, "completed", "V001", "s3://landing/trips.csv", None)
    ], schema=test_schemas["trips"])

    clean_df, bad_df = standardize_and_validate(mock_raw_trips, mock_routes, mock_vehicles, "test_sfn_execution")

    assert clean_df.count() == 0
    assert bad_df.count() == 2

    # Assert reasons
    reasons = [row["quarantine_reason"] for row in bad_df.collect()]
    for reason in reasons:
        assert "Temporal validation failure" in reason
