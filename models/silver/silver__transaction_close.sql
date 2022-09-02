{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['_INSERTED_TIMESTAMP::DATE']
) }}

WITH base AS (

    SELECT
        tx_group_id,
        tx_id,
        block_id,
        intra,
        inner_tx,
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
        {{ ref('silver__transaction') }}
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
        intra,
        block_id,
        tx_group_id,
        tx_id,
        inner_tx,
        algorand_decode_b64_addr(account) account,
        asset_id,
        SUM(amount) amount,
        _INSERTED_TIMESTAMP
    FROM
        base A
    GROUP BY
        intra,
        block_id,
        tx_group_id,
        tx_id,
        inner_tx,
        account,
        asset_id,
        _INSERTED_TIMESTAMP
)
SELECT
    block_id,
    intra,
    tx_group_id,
    tx_id,
    inner_tx,
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
