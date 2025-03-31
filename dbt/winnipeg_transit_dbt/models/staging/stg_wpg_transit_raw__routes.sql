WITH source AS(
    SELECT * FROM {{ source('winnipeg-transit-data-pipeline', 'routes') }}
),
renamed AS(
    
SELECT 
        key,
        number AS route_number,
        name AS route_name,
        variant AS route_variant,
        "customer-type" AS route_customer_type,
        coverage AS route_coverage,
        "badge-label" AS route_badge_label,
        "badge-style_class-names_class-name" AS route_badge_style_class_name,
        "badge-style_background-color" AS route_badge_style_background_color,
        "badge-style_border-color" AS route_badge_style_border_color,
        "badge-style_color" AS route_badge_style_color,
        TIMESTAMP(timestamp_fetched) AS timestamp_fetched,
           -- dates
        DATE_TRUNC(timestamp_fetched, DAY) as created_date,
        TIME(timestamp_fetched) as created_time

    FROM 
        source
)
SELECT * FROM renamed