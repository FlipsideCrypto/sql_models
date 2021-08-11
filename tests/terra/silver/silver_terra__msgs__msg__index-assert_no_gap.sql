WITH source AS (
    SELECT
        chain_id,
        block_id,
        tx_id,
        msg_index,
        LAG(
            msg_index,
            1
        ) over (
            PARTITION BY chain_id,
            block_id,
            tx_id
            ORDER BY
                msg_index ASC
        ) AS prev_msg_index
    FROM
        {{ ref('silver_terra__msgs') }}
),
tmp AS (
    SELECT
        chain_id,
        block_id,
        tx_id,
        prev_msg_index,
        msg_index,
        msg_index - prev_msg_index AS gap
    FROM
        source
    WHERE
        msg_index - prev_msg_index <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
