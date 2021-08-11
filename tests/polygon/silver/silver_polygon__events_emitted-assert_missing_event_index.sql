WITH source AS (
    SELECT
        A.value :block_id :: INT AS block_id,
        A.value :event_index :: INT AS event_index
    FROM
        {{ ref('polygon_dbt__events_emitted') }},
        LATERAL FLATTEN(
            input => record_content :results
        ) A
)
SELECT
    block_id,
    event_index
FROM
    {{ ref('silver_polygon__events_emitted') }}
