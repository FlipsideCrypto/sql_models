WITH source AS (
    SELECT
        chain_id,
        block_id,
        tx_id,
        log_index,
        LAG(
            log_index,
            1
        ) over (
            PARTITION BY chain_id,
            block_id,
            tx_id
            ORDER BY
                log_index ASC
        ) AS prev_log_index
    FROM
        {{ ref('silver_polygon__udm_events') }}
),
tmp AS (
    SELECT
        chain_id,
        block_id,
        tx_id,
        prev_log_index,
        log_index,
        log_index - prev_log_index AS gap
    FROM
        source
    WHERE
        log_index - prev_log_index <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
