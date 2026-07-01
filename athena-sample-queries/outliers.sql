-- Query to identify statistical duration and distance outliers for a specific query date
-- Computes baseline statistics over a 7-day look-back window relative to the query date

WITH trips_source AS (
    SELECT *
    FROM "edai_city_transit_pipeline_dev_silver_db"."silver_trips_active" FOR VERSION AS OF 'test_branch'
    WHERE CAST(start_time AS DATE) BETWEEN DATE '{query_date}' - INTERVAL '7' DAY AND DATE '{query_date}'
),
route_stats AS (
    -- Calculate baseline duration statistics over the past 7 days ending on query_date
    SELECT 
        t.route_id,
        v.vehicle_type,
        AVG(date_diff('minute', t.start_time, t.end_time)) AS avg_duration_minutes,
        STDDEV(date_diff('minute', t.start_time, t.end_time)) AS stddev_duration_minutes
    FROM 
        trips_source t
    JOIN 
        edai_city_transit_pipeline_dev_landing_db.raw_vehicles v ON t.vehicle_id = v.vehicle_id
    WHERE 
        t.status = 'completed'
        AND t.end_time IS NOT NULL
    GROUP BY 
        1, 2
),
current_trips AS (
    -- Scope target trips to evaluate strictly to the query date
    SELECT 
        t.trip_id,
        t.route_id,
        t.vehicle_id,
        v.vehicle_type,
        t.start_time,
        t.end_time,
        t.distance_km,
        date_diff('minute', t.start_time, t.end_time) AS duration_minutes
    FROM 
        trips_source t
    JOIN 
        edai_city_transit_pipeline_dev_landing_db.raw_vehicles v ON t.vehicle_id = v.vehicle_id
    WHERE 
        CAST(t.start_time AS DATE) = DATE '{query_date}'
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
        COALESCE(rs.stddev_duration_minutes, 0.0) AS stddev_duration_minutes,
        CASE 
            WHEN COALESCE(rs.stddev_duration_minutes, 0.0) > 0.0 
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
        ELSE NULL
    END AS outlier_reason
FROM 
    calculated_outliers
WHERE 
    z_score > 3.0 
    OR distance_km > 80.0;
