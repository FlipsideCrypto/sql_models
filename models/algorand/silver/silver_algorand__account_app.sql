{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'account_app','silver_algorand']
) }}

SELECT
  algorand_decode_hex_addr(
    addr :: text
  ) AS address,
  app AS app_id,
  deleted AS app_closed,
  closed_at AS closed_at,
  created_at AS created_at,
  localstate AS app_info,
  concat_ws(
    '-',
    addr :: STRING,
    app :: STRING
  ) AS _unique_key,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ source(
    'algorand',
    'ACCOUNT_APP'
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
