{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', ADDRESS, ASSET_ID)",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'asset_id' ]
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
  aa.addr :: STRING AS address,
  assetid AS asset_id,
  an.name :: STRING AS asset_name,
  amount AS amount,
  created_at AS asset_added_at,
  closed_at AS asset_last_removed,
  deleted AS asset_closed,
  frozen AS frozen,
  _FIVETRAN_SYNCED
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
AND _FIVETRAN_SYNCED >= (
  SELECT
    MAX(
      _FIVETRAN_SYNCED
    )
  FROM
    {{ this }}
)
{% endif %}
