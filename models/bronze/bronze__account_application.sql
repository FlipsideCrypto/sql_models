{{ config (
  materialized = 'view'
) }}

SELECT
  addr,
  app,
  deleted,
  closed_at,
  created_at,
  localstate,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP,
  __HEVO__LOADED_AT
FROM
  {{ source(
    'algorand',
    'ACCOUNT_APP'
  ) }}
