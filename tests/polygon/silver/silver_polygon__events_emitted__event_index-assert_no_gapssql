WITH source AS (
    SELECT
        block_id,
        event_index,
        LAG(
            event_index,
            1
        ) over(
            PARTITION BY block_id
            ORDER BY
                event_index ASC
        ) AS prev_event_index
    FROM
        {{ ref('silver_polygon__events_emitted') }}
),
tmp AS (
    SELECT
        block_id,
        event_index,
        event_index - prev_event_index AS gap,
        prev_event_index
    FROM
        source
    WHERE
        event_index - prev_event_index <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
