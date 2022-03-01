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
  algorand_decode_hex_addr(to_char(aa.addr, 'base64')) AS address,
  aa.assetid AS asset_id,
  an.name :: STRING AS asset_name,
  aa.amount AS amount,
  aa.created_at AS asset_added_at,
  ab.block_timestamp AS added_at_timestamp,
  aa.closed_at AS asset_last_removed,
  aa.deleted AS asset_closed,
  aa.frozen AS frozen,
  concat_ws(
    '-',
    address :: STRING,
    asset_id :: STRING
  ) AS _unique_key,
  aa.__HEVO__LOADED_AT
FROM
  {{ source(
    'algorand',
    'ACCOUNT_ASSET'
  ) }}
  aa
  LEFT JOIN asset_name an
  ON aa.assetid = an.index
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
