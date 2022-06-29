{{ config (
  materialized = 'view'
) }}

SELECT
  ROUND,
  realtime,
  rewardslevel,
  header,
  __HEVO__LOADED_AT,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _inserted_timestamp
FROM
  {{ source(
    'algorand',
    'BLOCK_HEADER'
  ) }}
