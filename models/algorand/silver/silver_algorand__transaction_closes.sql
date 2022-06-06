{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, intra, account)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        tx_ID,
        DATA
    FROM
        {{ source(
            'algorand_db_external',
            'algorand_indexer_tx'
        ) }}

{% if is_incremental() %}
WHERE
    _PARTITION_BY_DATE >= CURRENT_DATE -2
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    _PARTITION_BY_DATE DESC)) = 1
),
inner_outer AS (
    SELECT
        A.tx_ID,
        A.data :"confirmed-round" :: INT AS block_id,
        A.data :"intra-round-offset" :: INT AS intra,
        e.this :"close-amount" :: INT amount,
        COALESCE(
            e.this :"asset-id" :: INT,
            0
        ) asset_id,
        COALESCE(
            e.this :"close-remainder-to",
            e.this :"close-to"
        ) :: STRING account
    FROM
        base A,
        LATERAL FLATTEN(
            input => A.data,
            outer => TRUE,
            recursive => TRUE
        ) e
    WHERE
        e.key = 'close-amount'
        AND e.this :"close-amount" :: INT > 0
)
SELECT
    b.block_timestamp,
    A.intra,
    A.block_id,
    A.tx_id,
    A.account,
    A.asset_id,
    SUM(amount) amount
FROM
    inner_outer A
    JOIN {{ ref('silver_algorand__block') }}
    b
    ON A.block_ID = b.block_ID
GROUP BY
    b.block_timestamp,
    A.intra,
    A.block_id,
    A.tx_id,
    A.account,
    A.asset_id
