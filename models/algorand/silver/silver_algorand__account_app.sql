{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'account_app','silver_algorand']
) }}

SELECT
  algorand_decode_hex_addr(
    b.addr :: text
  ) AS address,
  b.app AS app_id,
  b.deleted AS app_closed,
  b.closed_at AS closed_at,
  b.created_at AS created_at,
  ab.block_timestamp AS created_at_timestamp,
  b.localstate AS app_info,
  concat_ws(
    '-',
    b.addr :: STRING,
    b.app :: STRING
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
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON b.created_at = ab.block_id
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
