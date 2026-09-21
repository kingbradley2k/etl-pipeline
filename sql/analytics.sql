-- Kenyan Weather ETL Pipeline: analytical queries
-- All day-based queries use Africa/Nairobi dates because the monitored cities
-- are Kenyan, while `observed_at` remains stored in UTC for consistency.

-- 1. Average temperature by day across all locations.
SELECT
    (observed_at AT TIME ZONE 'Africa/Nairobi')::DATE AS observation_date,
    ROUND(AVG(temperature_c), 2) AS average_temperature_c
FROM weather_observations
GROUP BY observation_date
ORDER BY observation_date;

-- 2. Minimum and maximum temperature by day across all locations.
SELECT
    (observed_at AT TIME ZONE 'Africa/Nairobi')::DATE AS observation_date,
    MIN(temperature_c) AS minimum_temperature_c,
    MAX(temperature_c) AS maximum_temperature_c
FROM weather_observations
GROUP BY observation_date
ORDER BY observation_date;

-- 3. Average temperature by location.
SELECT
    location.city,
    location.country_code,
    ROUND(AVG(observation.temperature_c), 2) AS average_temperature_c
FROM weather_observations AS observation
JOIN locations AS location ON location.location_id = observation.location_id
GROUP BY location.city, location.country_code
ORDER BY average_temperature_c DESC, location.city;

-- 4. Average humidity by location.
SELECT
    location.city,
    location.country_code,
    ROUND(AVG(observation.humidity_pct), 2) AS average_humidity_pct
FROM weather_observations AS observation
JOIN locations AS location ON location.location_id = observation.location_id
GROUP BY location.city, location.country_code
ORDER BY average_humidity_pct DESC, location.city;

-- 5. Number of observations captured for each configured location.
SELECT
    location.city,
    location.country_code,
    COUNT(observation.observation_id) AS observation_count
FROM locations AS location
LEFT JOIN weather_observations AS observation
    ON observation.location_id = location.location_id
GROUP BY location.location_id, location.city, location.country_code
ORDER BY observation_count DESC, location.city;

-- 6. Weather-condition distribution, including records without a condition.
SELECT
    COALESCE(weather_condition, 'Unknown') AS weather_condition,
    COUNT(*) AS observation_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage_of_observations
FROM weather_observations
GROUP BY COALESCE(weather_condition, 'Unknown')
ORDER BY observation_count DESC, weather_condition;

-- 7. Daily temperature range by location (maximum minus minimum temperature).
SELECT
    location.city,
    (observation.observed_at AT TIME ZONE 'Africa/Nairobi')::DATE AS observation_date,
    MIN(observation.temperature_c) AS minimum_temperature_c,
    MAX(observation.temperature_c) AS maximum_temperature_c,
    ROUND(MAX(observation.temperature_c) - MIN(observation.temperature_c), 2)
        AS temperature_range_c
FROM weather_observations AS observation
JOIN locations AS location ON location.location_id = observation.location_id
GROUP BY location.city, observation_date
ORDER BY observation_date DESC, location.city;

-- 8. Latest observation for every location, including its measured conditions.
SELECT DISTINCT ON (location.location_id)
    location.city,
    location.country_code,
    observation.observed_at,
    observation.temperature_c,
    observation.humidity_pct,
    observation.wind_speed_kmh,
    observation.pressure_msl_hpa,
    observation.weather_condition
FROM locations AS location
JOIN weather_observations AS observation
    ON observation.location_id = location.location_id
ORDER BY location.location_id, observation.observed_at DESC;

-- 9. Locations with no observation in the last two hours. Adjust the interval
-- for a different scheduler cadence or operational alerting threshold.
SELECT
    location.city,
    location.country_code,
    MAX(observation.observed_at) AS latest_observation_at
FROM locations AS location
LEFT JOIN weather_observations AS observation
    ON observation.location_id = location.location_id
GROUP BY location.location_id, location.city, location.country_code
HAVING MAX(observation.observed_at) IS NULL
    OR MAX(observation.observed_at) < CURRENT_TIMESTAMP - INTERVAL '2 hours'
ORDER BY latest_observation_at NULLS FIRST, location.city;

-- 10. Pipeline success/failure statistics and average completed run duration.
SELECT
    status,
    COUNT(*) AS run_count,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
            FILTER (WHERE completed_at IS NOT NULL),
        2
    ) AS average_duration_seconds,
    SUM(records_extracted) AS total_records_extracted,
    SUM(records_loaded) AS total_records_loaded,
    SUM(records_rejected) AS total_records_rejected
FROM pipeline_runs
GROUP BY status
ORDER BY status;
