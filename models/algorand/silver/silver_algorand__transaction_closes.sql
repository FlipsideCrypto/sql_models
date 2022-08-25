{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_timestamp,
        tx_ID,
        block_id,
        intra,
        COALESCE(
            tx_message :ca :: INT,
            tx_message :aca :: INT
        ) amount,
        COALESCE(
            tx_message :txn :close :: STRING,
            tx_message :txn :aclose :: STRING
        ) account,
        COALESCE(
            tx_message :txn :xaid :: INT,
            0
        ) asset_id,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver_algorand__transactions') }}
    WHERE
        COALESCE(
            tx_message :txn :close :: STRING,
            tx_message :txn :aclose :: STRING
        ) IS NOT NULL
        AND COALESCE(
            tx_message :ca :: INT,
            tx_message :aca :: INT
        ) > 0

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
)
{% endif %}
),
mid AS (
    SELECT
        block_timestamp,
        intra,
        block_id,
        tx_id,
        algorand_decode_b64_addr(account) account,
        asset_id,
        SUM(amount) amount,
        _INSERTED_TIMESTAMP
    FROM
        base A
    GROUP BY
        block_timestamp,
        intra,
        block_id,
        tx_id,
        account,
        asset_id,
        _INSERTED_TIMESTAMP
)
SELECT
    block_timestamp,
    intra,
    block_id,
    tx_id,
    account,
    asset_id,
    amount,
    concat_ws(
        '-',
        block_id,
        intra,
        account,
        asset_ID
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
FROM
    mid
