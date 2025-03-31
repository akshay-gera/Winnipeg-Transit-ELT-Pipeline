WITH source AS(
    SELECT * FROM {{ source('winnipeg-transit-data-pipeline', 'stop_features') }}
),
renamed AS(
    SELECT
        stop_number,
        feature_name AS stop_feature_name,
        feature_count AS stop_feature_count,
        TIMESTAMP(timestamp_fetched) AS timestamp_fetched,
        -- dates
        DATE_TRUNC(timestamp_fetched, DAY) as created_date,
        TIME(timestamp_fetched) as created_time

    FROM source
)
SELECT * FROM renamed