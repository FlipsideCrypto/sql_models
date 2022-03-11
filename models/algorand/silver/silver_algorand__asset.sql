{{ config(
  materialized = 'incremental',
  unique_key = 'asset_id',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'asset', 'silver_algorand']
) }}

SELECT
  INDEX AS asset_id,
  algorand_decode_hex_addr(
    creator_addr :: text
  ) AS creator_address,
  params :t :: NUMBER AS total_supply,
  params :an :: STRING AS asset_name,
  params :au :: STRING AS asset_url,
  CASE
    WHEN params :dc IS NULL THEN 0
    WHEN params :dc IS NOT NULL THEN params :dc :: NUMBER
  END AS decimals,
  deleted AS asset_deleted,
  closed_at AS closed_at,
  created_at AS created_at,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ source(
    'algorand',
    'ASSET'
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
