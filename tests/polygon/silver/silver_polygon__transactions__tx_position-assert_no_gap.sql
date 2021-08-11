WITH source AS (
    SELECT
        block_id,
        tx_position,
        LAG(
            tx_position,
            1
        ) over(
            PARTITION BY block_id
            ORDER BY
                tx_position ASC
        ) AS prev_tx_position
    FROM
        {{ ref('silver_polygon__transactions') }}
),
tmp AS (
    SELECT
        block_id,
        prev_tx_position,
        tx_position,
        tx_position - prev_tx_position AS gap
    FROM
        source
    WHERE
        tx_position - prev_tx_position <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
