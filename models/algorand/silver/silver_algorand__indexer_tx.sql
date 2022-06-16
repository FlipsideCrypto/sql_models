{{ config(
    materialized = 'incremental',
    unique_key = 'TX_ID',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_PARTITION_BY_DATE::DATE']
) }}

SELECT
    tx_id,
    account_id,
    DATA :"confirmed-round" :: STRING AS block_id,
    DATA,
    _PARTITION_BY_DATE
FROM
    {{ source(
        'algorand_db_external',
        'algorand_indexer_tx'
    ) }}
WHERE
    1 = 1

{% if is_incremental() %}
AND _PARTITION_BY_DATE >= (
    SELECT
        MAX(
            _PARTITION_BY_DATE
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _PARTITION_BY_DATE DESC)) = 1
