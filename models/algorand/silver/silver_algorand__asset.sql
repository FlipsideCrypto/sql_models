{{ config(
  materialized = 'incremental',
  unique_key = 'asset_id',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'asset', 'silver_algorand']
) }}

WITH asset_config AS(

  SELECT
    DISTINCT asset_id,
    asset_parameters :an :: STRING AS asset_name,
    asset_parameters :t :: NUMBER AS asset_amount,
    CASE
      WHEN asset_parameters :dc :: NUMBER IS NULL THEN 0
      ELSE asset_parameters :dc :: NUMBER
    END AS decimals
  FROM
    {{ ref('silver_algorand__asset_configuration_transaction') }}
  WHERE
    tx_message :caid :: NUMBER IS NULL
    AND asset_parameters :an :: STRING IS NOT NULL
    AND asset_parameters IS NOT NULL
)
SELECT
  A.index AS asset_id,
  algorand_decode_hex_addr(
    creator_addr :: text
  ) AS creator_address,
  CASE
    WHEN A.deleted = 'TRUE'
    AND ac.asset_id IS NOT NULL THEN ac.asset_amount
    ELSE A.params :t :: NUMBER
  END AS total_supply,
  CASE
    WHEN A.deleted = 'TRUE'
    AND ac.asset_id IS NOT NULL THEN ac.asset_name
    ELSE A.params :an :: STRING
  END AS asset_name,
  A.params :au :: STRING AS asset_url,
  CASE
    WHEN A.deleted = 'TRUE'
    AND ac.asset_id IS NOT NULL THEN ac.decimals
    WHEN A.params :dc IS NULL THEN 0
    WHEN A.params :dc IS NOT NULL THEN params :dc :: NUMBER
  END AS decimals,
  A.deleted AS asset_deleted,
  A.closed_at AS closed_at,
  A.created_at AS created_at,
  DATEADD(
    ms,
    A.__HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ source(
    'algorand',
    'ASSET'
  ) }} A
  LEFT JOIN asset_config ac
  ON A.index = ac.asset_id
WHERE
  1 = 1

{% if is_incremental() %}
AND DATEADD(
  ms,
  A.__HEVO__LOADED_AT,
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
