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
    __HEVO__LOADED_AT
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
  __HEVO__LOADED_AT
FROM
  allBLOCKS
WHERE
  1 = 1

{% if is_incremental() %}
AND __HEVO__LOADED_AT >= (
  SELECT
    MAX(
      __HEVO__LOADED_AT
    )
  FROM
    {{ this }}
)
{% endif %}
