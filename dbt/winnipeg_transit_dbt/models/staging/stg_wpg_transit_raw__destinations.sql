WITH source AS(
    SELECT * FROM {{ source('winnipeg-transit-data-pipeline', 'destinations') }}
),
renamed AS(
    SELECT

        variant_key AS route_variant,
        destination_name,
        destination_id,
        TIMESTAMP(timestamp_fetched) AS timestamp_fetched,
        -- dates
        DATE_TRUNC(timestamp_fetched, DAY) as created_date,
        TIME(timestamp_fetched) as created_time

    FROM source
)
SELECT * FROM renamed