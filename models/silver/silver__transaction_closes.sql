{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        tx_ID,
        DATA,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__indexer_tx') }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1
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
        ) :: STRING account,
        _INSERTED_TIMESTAMP
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
    SUM(amount) amount,
    concat_ws(
        '-',
        A.block_id,
        A.intra,
        A.account,
        A.asset_ID
    ) AS _unique_key,
    A._INSERTED_TIMESTAMP
FROM
    inner_outer A
    JOIN {{ ref('core__dim_block') }}
    b
    ON A.block_ID = b.block_ID
GROUP BY
    b.block_timestamp,
    A.intra,
    A.block_id,
    A.tx_id,
    A.account,
    A.asset_id,
    _unique_key,
    A._INSERTED_TIMESTAMP
