WITH source AS (
    SELECT
        chain_id, block_id,
        LAG(
            chain_id, block_id,
            1
        ) over (
            ORDER BY
                chain_id, block_id ASC
        ) AS prev_chain_id, block_id
    FROM
        {{ ref('silver_terra__blocks') }}
),
tmp AS (
    SELECT
        prev_chain_id, block_id,
        chain_id, block_id,
        chain_id, block_id - prev_chain_id, block_id AS gap
    FROM
        source
    WHERE
        chain_id, block_id - prev_chain_id, block_id <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
