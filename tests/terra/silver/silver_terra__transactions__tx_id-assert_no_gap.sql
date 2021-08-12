WITH source AS (
    SELECT
        chain_id,
        block_id,
        tx_id,
        LAG(
            tx_id,
            1
        ) over (
            PARTITION BY chain_id,
            block_id
            ORDER BY
                tx_id ASC
        ) AS prev_tx_id
    FROM
        {{ ref('silver_terra__transactions') }}
),
tmp AS (
    SELECT
        chain_id,
        block_id,
        prev_tx_id,
        tx_id,
        tx_id - prev_tx_id AS gap
    FROM
        source
    WHERE
        tx_id - prev_tx_id <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
