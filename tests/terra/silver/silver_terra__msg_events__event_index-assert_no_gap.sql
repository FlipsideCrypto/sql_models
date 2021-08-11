WITH source AS (
    SELECT
        chain_id,
        tx_id,
        block_id,
        event_type,
        event_index,
        LAG(
            event_index,
            1
        ) over (
            PARTITION BY chain_id,
            tx_id,
            block_id,
            event_type
            ORDER BY
                event_index ASC
        ) AS prev_event_index
    FROM
        {{ ref('silver_terra__msg_events') }}
),
tmp AS (
    SELECT
        chain_id,
        tx_id,
        block_id,
        event_type,
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
