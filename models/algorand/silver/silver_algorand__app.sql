{{ config(
  materialized = 'incremental',
  unique_key = 'APP_ID',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'app', 'silver_algorand']
) }}

SELECT
  INDEX AS app_id,
  algorand_decode_hex_addr(to_char(aa.creator, 'base64')) AS creator_address,
  aa.deleted AS app_closed,
  aa.closed_at AS closed_at,
  aa.created_at AS created_at,
  ab.block_timestamp AS created_at_timestamp,
  aa.params,
  aa.__HEVO__LOADED_AT
FROM
  {{ source(
    'algorand',
    'APP'
  ) }}
  aa
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON aa.created_at = ab.block_id
WHERE
  1 = 1

{% if is_incremental() %}
AND aa.__HEVO__LOADED_AT >= (
  SELECT
    MAX(
      __HEVO__LOADED_AT
    )
  FROM
    {{ this }}
)
{% endif %}
