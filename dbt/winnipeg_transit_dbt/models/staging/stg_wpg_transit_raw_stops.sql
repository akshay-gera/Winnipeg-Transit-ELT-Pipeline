WITH source AS(
    SELECT * FROM {{ source('winnipeg-transit-data-pipeline', 'stops') }}
),
renamed AS(
    SELECT
        variant_key AS _variant,
        stop_id,
        stop_name,
        stop_number,
        direction AS stop_direction,
        side AS stop_side,
        street_key AS stop_street_key,
        street_name AS stop_street_name,
        cross_street_key AS stop_cross_street_key,
        cross_street_name AS stop_cross_street_name,
        cross_street_leg AS stop_cross_street_leg,
        latitude AS stop_latitude,
        longitude AS stop_longitude,
        TIMESTAMP(timestamp_fetched) AS timestamp_fetched,
        -- dates
        DATE_TRUNC(timestamp_fetched, DAY) as created_date,
        TIME(timestamp_fetched) as created_time

    FROM source
)
SELECT * FROM renamed