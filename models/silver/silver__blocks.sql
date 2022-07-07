{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'merge'
) }}

SELECT
    ROUND AS block_id,
    realtime :: TIMESTAMP AS block_timestamp,
    rewardslevel AS rewardslevel,
    header :gen :: STRING AS network,
    header :gh :: STRING AS genesis_hash,
    header :prev :: STRING AS prev_block_hash,
    header :txn :: STRING AS txn_root,
    header,
    DATEADD(
        ms,
        __HEVO__LOADED_AT,
        '1970-01-01'
    ) AS _inserted_timestamp
FROM
    {{ ref('bronze__blocks') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _INSERTED_TIMESTAMP
        )
    FROM
        {{ this }}
)
{% endif %}
