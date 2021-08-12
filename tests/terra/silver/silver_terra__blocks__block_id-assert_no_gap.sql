WITH source AS (
    SELECT
        chain_id,
        block_id,
        LAG(
            block_id,
            1
        ) over(
            ORDER BY
                block_id ASC
        ) AS prev_block_id
    FROM
        {{ ref('silver_terra__blocks') }}
),
tmp AS (
    SELECT
        chain_id,
        prev_block_id,
        block_id,
        block_id - prev_block_id AS gap
    FROM
        source
    WHERE
        block_id - prev_block_id <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
