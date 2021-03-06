{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'asset_id','silver_algorand']
) }}

WITH asset_name AS (

  SELECT
    DISTINCT(INDEX) AS INDEX,
    params :an :: STRING AS NAME
  FROM
    {{ source(
      'algorand',
      'ASSET'
    ) }}
)
SELECT
  algorand_decode_hex_addr(
    aa.addr :: text
  ) AS address,
  assetid AS asset_id,
  an.name :: STRING AS asset_name,
  amount :: NUMBER AS amount,
  created_at AS asset_added_at,
  closed_at AS asset_last_removed,
  deleted AS asset_closed,
  frozen AS frozen,
  concat_ws(
    '-',
    address :: STRING,
    asset_id :: STRING
  ) AS _unique_key,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ source(
    'algorand',
    'ACCOUNT_ASSET'
  ) }}
  aa
  LEFT JOIN asset_name an
  ON aa.assetid = an.index
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
