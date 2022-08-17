{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    ROUND AS block_id,
    realtime :: TIMESTAMP AS block_timestamp,
    realtime :: DATE AS block_date,
    HOUR(realtime) AS block_hour,
    week(realtime) AS block_week,
    MONTH(realtime) AS block_month,
    quarter(realtime) AS block_quarter,
    YEAR(realtime) AS block_year,
    dayofmonth(realtime) AS block_DAYOFMONTH,
    dayofweek(realtime) AS block_DAYOFWEEK,
    dayofyear(realtime) AS block_DAYOFYEAR,
    rewardslevel AS rewards_level,
    header :gen :: STRING AS network,
    header :gh :: STRING AS genesis_hash,
    header :prev :: STRING AS prev_block_hash,
    header :txn :: STRING AS txn_root,
    header,
    DATEADD(
        ms,
        __HEVO__LOADED_AT,
        '1970-01-01'
    ) AS _INSERTED_TIMESTAMP
FROM
    {{ ref('bronze__block') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
