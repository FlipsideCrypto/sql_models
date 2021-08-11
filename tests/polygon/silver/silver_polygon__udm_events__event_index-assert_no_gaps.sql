WITH source AS (
    SELECT
        block_id,
        tx_id,
        log_index,
        LAG(
            log_index,
            1
        ) over(
            PARTITION BY block_id,
            tx_id
            ORDER BY
                log_index ASC
        ) AS prev_event_index
    FROM
        {{ ref('silver_polygon__udm_events') }}
),
tmp AS (
    SELECT
        block_id,
        tx_id,
        log_index,
        log_index - prev_event_index AS gap,
        prev_event_index
    FROM
        source
    WHERE
        log_index - prev_event_index <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
