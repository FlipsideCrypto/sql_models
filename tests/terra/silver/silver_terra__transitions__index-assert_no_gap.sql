WITH source AS (
    SELECT
        chain_id,
        block_id,
        transition_type,
        "INDEX",
        LAG(
            "INDEX",
            1
        ) over (
            PARTITION BY chain_id,
            block_id,
            transition_type
            ORDER BY
                "INDEX" ASC
        ) AS prev_msg_index
    FROM
        {{ ref('silver_terra__msgs') }}
),
tmp AS (
    SELECT
        chain_id,
        block_id,
        transition_type,
        prev_msg_index,
        "INDEX",
        "INDEX" - prev_msg_index AS gap
    FROM
        source
    WHERE
        "INDEX" - prev_msg_index <> 1
)
SELECT
    *
FROM
    tmp
ORDER BY
    gap DESC
