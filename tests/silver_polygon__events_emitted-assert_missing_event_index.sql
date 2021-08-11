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
        ) AS prev_event
    FROM
        {{ ref('silver_polygon__events_emitted') }}
),
tmp AS (
    SELECT
        block_id,
        prev_event,
        event_index - prev_event AS gap,
        event_index - prev_event - 1 AS missing_events
    FROM
        source
    WHERE
        event_index - prev_event <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    3 DESC
