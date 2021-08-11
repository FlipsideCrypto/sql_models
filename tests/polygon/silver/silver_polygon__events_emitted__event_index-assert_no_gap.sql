WITH source AS (
    SELECT
        chain_id,
        block_id,
        tx_id,
        event_index,
        LAG(
            event_index,
            1
        ) over (
            PARTITION BY chain_id,
            block_id,
            tx_id
            ORDER BY
                event_index ASC
        ) AS prev_event_index
    FROM
        {{ ref('silver_polygon__events_emitted') }}
),
tmp AS (
    SELECT
        chain_id,
        block_id,
        tx_id,
        prev_event_index,
        event_index,
        event_index - prev_event_index AS gap
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
