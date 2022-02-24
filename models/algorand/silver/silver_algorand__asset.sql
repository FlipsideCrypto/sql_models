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
  params :dc AS decimals,
  deleted AS asset_deleted,
  closed_at AS closed_at,
  created_at AS created_at,
  _FIVETRAN_SYNCED
FROM
  {{ source(
    'algorand_patch',
    'ASSET'
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
