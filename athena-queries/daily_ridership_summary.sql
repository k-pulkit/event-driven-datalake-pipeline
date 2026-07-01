-- Query to calculate daily ridership summary by trip_date, route_id, and vehicle_type
-- Employs dynamic partition pruning on start_time and window functions to find hourly peaks

WITH hourly_counts AS (
    SELECT 
        CAST(t.start_time AS DATE) AS trip_date,
        t.route_id,
        v.vehicle_type,
        EXTRACT(HOUR FROM t.start_time) AS trip_hour,
        COUNT(t.trip_id) AS hour_trip_count
    FROM 
        {silver_database}.silver_trips_active t
    JOIN 
        {landing_database}.raw_vehicles v ON t.vehicle_id = v.vehicle_id
    WHERE 
        CAST(t.start_time AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    GROUP BY 
        1, 2, 3, 4
),
ranked_hours AS (
    SELECT 
        trip_date,
        route_id,
        vehicle_type,
        trip_hour,
        ROW_NUMBER() OVER (
            PARTITION BY trip_date, route_id, vehicle_type 
            ORDER BY hour_trip_count DESC, trip_hour ASC
        ) as rn
    FROM 
        hourly_counts
),
base_metrics AS (
    SELECT 
        CAST(t.start_time AS DATE) AS trip_date,
        t.route_id,
        v.vehicle_type,
        COUNT(t.trip_id) AS total_trips,
        COUNT(DISTINCT t.vehicle_id) AS unique_vehicles,
        AVG(v.capacity) AS avg_capacity,
        AVG(date_diff('minute', t.start_time, t.end_time)) AS avg_duration_minutes,
        MAX(date_diff('minute', t.start_time, t.end_time)) AS max_duration_minutes,
        SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) AS trips_completed,
        SUM(CASE WHEN t.status = 'in_progress' THEN 1 ELSE 0 END) AS trips_in_progress
    FROM 
        {silver_database}.silver_trips_active t
    JOIN 
        {landing_database}.raw_vehicles v ON t.vehicle_id = v.vehicle_id
    WHERE 
        CAST(t.start_time AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
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
        rh.rn = 1;
