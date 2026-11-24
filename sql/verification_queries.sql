-- SQL verification queries for data/output.db
-- Run:  sqlite3 data/output.db < sql/verification_queries.sql
--   or: python -m src.loader --verify      (no sqlite3 tool needed)
.headers on
.mode column

-- 1. Number of stored records
SELECT COUNT(*) AS stored_records FROM earthquakes;

-- 2. Duplicate check: must return 0 (event_id is the PRIMARY KEY)
SELECT COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_event_ids FROM earthquakes;

-- 3. Required fields must not be empty: all counts must be 0
SELECT
    SUM(event_time IS NULL) AS null_time,
    SUM(magnitude IS NULL)  AS null_magnitude,
    SUM(latitude IS NULL)   AS null_latitude,
    SUM(longitude IS NULL)  AS null_longitude,
    SUM(depth_km IS NULL)   AS null_depth
FROM earthquakes;

-- 4. Value ranges: magnitude, coordinates, depth and time period of the data
SELECT
    MIN(magnitude) AS min_mag,  MAX(magnitude) AS max_mag,
    MIN(latitude)  AS min_lat,  MAX(latitude)  AS max_lat,
    MIN(longitude) AS min_lon,  MAX(longitude) AS max_lon,
    MIN(depth_km)  AS min_depth, MAX(depth_km) AS max_depth,
    MIN(event_time) AS first_event, MAX(event_time) AS last_event
FROM earthquakes;

-- 5. Test records from validator.py must not be in the database: must return 0
SELECT COUNT(*) AS test_records_in_db FROM earthquakes WHERE event_id LIKE 'test%';

-- 6. Example rows
SELECT event_id, event_time, magnitude, mag_type, depth_km, region
FROM earthquakes
ORDER BY event_time
LIMIT 5;

-- 7. Ten strongest events
SELECT event_time, magnitude, depth_km, place, alert, tsunami
FROM earthquakes
ORDER BY magnitude DESC
LIMIT 10;

-- 8. Events by magnitude class
SELECT
    CASE
        WHEN magnitude < 4 THEN '1) minor (2.5-3.9)'
        WHEN magnitude < 5 THEN '2) light (4.0-4.9)'
        WHEN magnitude < 6 THEN '3) moderate (5.0-5.9)'
        WHEN magnitude < 7 THEN '4) strong (6.0-6.9)'
        ELSE '5) major (7.0+)'
    END AS magnitude_class,
    COUNT(*) AS events,
    ROUND(AVG(depth_km), 1) AS avg_depth_km
FROM earthquakes
GROUP BY magnitude_class
ORDER BY magnitude_class;

-- 9. Events by depth class
SELECT
    CASE
        WHEN depth_km < 70  THEN '1) shallow (<70 km)'
        WHEN depth_km < 300 THEN '2) intermediate (70-300 km)'
        ELSE '3) deep (300+ km)'
    END AS depth_class,
    COUNT(*) AS events,
    ROUND(AVG(magnitude), 2) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude
FROM earthquakes
GROUP BY depth_class
ORDER BY depth_class;

-- 10. Top 10 regions by number of events
SELECT region,
       COUNT(*) AS events,
       ROUND(AVG(magnitude), 2) AS avg_magnitude,
       MAX(magnitude) AS max_magnitude
FROM earthquakes
GROUP BY region
ORDER BY events DESC
LIMIT 10;

-- 11. Events by magnitude type
SELECT mag_type, COUNT(*) AS events, ROUND(AVG(magnitude), 2) AS avg_magnitude
FROM earthquakes
GROUP BY mag_type
ORDER BY events DESC;

-- 12. Events per day
SELECT DATE(event_time) AS day, COUNT(*) AS events, MAX(magnitude) AS max_magnitude
FROM earthquakes
GROUP BY day
ORDER BY day;

-- 13. PAGER alerts and tsunami flags
SELECT COALESCE(alert, 'no alert') AS alert,
       COUNT(*) AS events,
       SUM(tsunami) AS tsunami_flags
FROM earthquakes
GROUP BY alert
ORDER BY events DESC;
