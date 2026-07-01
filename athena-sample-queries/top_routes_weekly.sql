-- Query to identify the top 10 routes by trip volume over the past 7 days
-- Enables S3 partition pruning by filtering on start_time

WITH trips_source AS (
    SELECT *
    FROM "edai_city_transit_pipeline_dev_silver_db"."silver_trips_active" FOR VERSION AS OF 'test_branch'
    -- Centralized partition filter for the lookback window
    WHERE start_time >= current_date - INTERVAL '7' DAY
),
weekly_totals AS (
    SELECT 
        route_id,
        COUNT(trip_id) AS total_trips,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS total_completed_trips,
        ROUND(AVG(date_diff('minute', start_time, end_time)), 1) AS avg_trip_duration_minutes
    FROM 
        trips_source
    GROUP BY 
        route_id
),
ranked_routes AS (
    SELECT 
        route_id,
        total_trips,
        total_completed_trips,
        avg_trip_duration_minutes,
        DENSE_RANK() OVER (ORDER BY total_trips DESC, route_id ASC) AS route_rank
    FROM 
        weekly_totals
)
SELECT 
    route_rank,
    route_id,
    total_trips,
    total_completed_trips,
    avg_trip_duration_minutes
FROM 
    ranked_routes
WHERE 
    route_rank <= 10;
