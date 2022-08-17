{{ config(
    materialized = 'incremental',
    unique_key = 'fact_block_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('silver__block') }}

{% if is_incremental() %}
WHERE
    _INSERTED_TIMESTAMP >= (
        SELECT
            MAX(
                _INSERTED_TIMESTAMP
            )
        FROM
            {{ this }}
    ) - INTERVAL '4 HOURS'
    OR block_id IN (
        SELECT
            block_id
        FROM
            {{ this }}
        WHERE
            dim_block_id = '-1'
    )
{% endif %}
),
txn AS (
    SELECT
        A.block_id,
        COUNT(
            DISTINCT intra
        ) AS tx_count,
        COUNT(
            DISTINCT sender
        ) AS tx_sender_count,
        SUM(fee) AS fee_total
    FROM
        {{ ref('silver__transaction') }} A
        JOIN base b
        ON A.block_id = b.block_id
    GROUP BY
        A.block_id
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['a.block_id' ]
    ) }} AS fact_block_id,
    A.block_id,
    COALESCE(
        A.block_timestamp,
        '1900-01-01' :: DATE
    ) block_timestamp,
    COALESCE(
        C.dim_block_id,
        '-1'
    ) AS dim_block_id,
    COALESCE(
        tx_count,
        0
    ) AS tx_count,
    COALESCE(
        tx_sender_count,
        0
    ) AS tx_sender_count,
    COALESCE(
        fee_total,
        0
    ) AS fee_total,
    A._inserted_timestamp,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
    base A
    LEFT JOIN txn b
    ON A.block_id = b.block_id
    LEFT JOIN {{ ref('core__dim_block') }} C
    ON A.block_id = C.block_id
