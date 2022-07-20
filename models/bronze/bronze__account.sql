{{ config (
  materialized = 'view'
) }}

SELECT
  addr,
  deleted,
  rewardsbase,
  rewards_total,
  microalgos,
  closed_at,
  created_at,
  keytype,
  account_data,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP,
  __HEVO__LOADED_AT
FROM
  {{ source(
    'algorand',
    'ACCOUNT'
  ) }}
