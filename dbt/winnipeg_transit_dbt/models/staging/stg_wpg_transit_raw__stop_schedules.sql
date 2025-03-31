WITH source AS(
    SELECT * FROM {{ source('winnipeg-transit-data-pipeline', 'stop_schedules') }}
),
renamed AS(
    SELECT
        stop_number,
        stop_name,
        route_key, 
        route_name,
        route_number,
        variant_key AS route_variant,
        variant_name AS route_variant_name,
        bus_key,
        bus_bike_rack AS has_bus_bike_rack,
        bus_wifi AS has_bus_wifi,
        scheduled_stop_key,
        cancelled AS is_cancelled,
        TIMESTAMP(arrival_estimated) AS arrival_estimated,
        TIMESTAMP(arrival_scheduled) AS arrival_scheduled,
        TIMESTAMP(arrival_estimated) - TIMESTAMP(arrival_scheduled) AS arrival_time_difference,
        TIMESTAMP(departure_estimated) AS departure_estimated,
        TIMESTAMP(departure_scheduled) AS departure_scheduled,
        TIMESTAMP(departure_estimated) - TIMESTAMP(departure_scheduled) AS departure_time_difference,
        TIMESTAMP(timestamp_fetched) AS timestamp_fetched,
        -- dates
        DATE_TRUNC(timestamp_fetched, DAY) as created_date,
        TIME(timestamp_fetched) as created_time

    FROM source
),
with_arrival_status AS(
    SELECT
        *,
        CASE
            WHEN EXTRACT(SECOND FROM arrival_time_difference) = 0 THEN 'On Time'
            WHEN EXTRACT(SECOND FROM arrival_time_difference) > 0  THEN 'Late'
            ELSE 'Early'
        END AS arrival_status,
        CASE
            WHEN EXTRACT(SECOND FROM departure_time_difference) = 0 THEN 'On Time'
            WHEN EXTRACT(SECOND FROM departure_time_difference) > 0  THEN 'Late'
            ELSE 'Early'
        END AS departure_status
    FROM renamed
)
SELECT 
*
FROM with_arrival_status