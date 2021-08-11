WITH source AS (
    SELECT
        chain_id,
        tx_id,
        LAG(
            tx_id,
            1
        ) over (
            PARTITION BY chain_id,
            ORDER BY
                tx_id ASC
        ) AS prev_msg_index
    FROM
        {{ ref('silver_terra__msgs') }}
),
tmp AS (
    SELECT
        chain_id,
        prev_msg_index,
        tx_id,
        tx_id - prev_msg_index AS gap
    FROM
        source
    WHERE
        tx_id - prev_msg_index <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
