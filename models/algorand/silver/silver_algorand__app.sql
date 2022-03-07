{{ config(
  materialized = 'incremental',
  unique_key = 'APP_ID',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'app', 'silver_algorand']
) }}

SELECT
  INDEX AS app_id,
  algorand_decode_hex_addr(
    creator :: text
  ) AS creator_address,
  deleted AS app_closed,
  closed_at AS closed_at,
  created_at AS created_at,
  params,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ source(
    'algorand',
    'APP'
  ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND DATEADD(
  ms,
  __HEVO__LOADED_AT,
  '1970-01-01'
) >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
