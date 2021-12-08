{{ config(
  materialized = 'incremental',
  unique_key = 'block_id',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'block']
) }}

SELECT
  ROUND AS block_id,
  realtime :: TIMESTAMP AS block_timestamp,
  rewardslevel AS rewardslevel,
  header :gen :: STRING AS network,
  header :gh :: STRING AS genisis_hash,
  header :prev :: STRING AS prev_block_hash,
  header :txn :: STRING AS txn_root,
  header,
  _FIVETRAN_SYNCED
FROM
  {{ source(
    'algorand',
    'BLOCK_HEADER'
  ) }}
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
