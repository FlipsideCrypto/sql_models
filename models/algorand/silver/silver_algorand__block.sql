{{ config(
  materialized = 'incremental',
  unique_key = 'block_id',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'block', 'silver_algorand']
) }}

WITH allBLOCKS AS(

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
      'MS',
      __HEVO__LOADED_AT,
      '1970-01-01'
    ) AS _FIVETRAN_SYNCED
  FROM
    {{ source(
      'algorand',
      'BLOCK_HEADER'
    ) }}
)
SELECT
  block_id,
  block_timestamp,
  rewardslevel,
  network,
  genesis_hash,
  prev_block_hash,
  txn_root,
  header,
  _FIVETRAN_SYNCED
FROM
  allBLOCKS
WHERE
  1 = 1

{% if is_incremental() %}
AND _FIVETRAN_SYNCED >= (
  SELECT
    MAX(
      _FIVETRAN_SYNCED
    )
  FROM
    {{ this }}
)
{% endif %}
