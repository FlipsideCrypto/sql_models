{{ config(
  materialized = 'incremental',
  unique_key = 'address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['address'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__contracts']
) }}

WITH base AS (

  SELECT
    system_created_at,
    LOWER(address) AS address,
    meta,
    NAME
  FROM
    (
      SELECT
        system_created_at,
        address,
        meta,
        NAME
      FROM
        {{ ref('ethereum_dbt__contracts') }}
      WHERE
        meta IS NOT NULL
      UNION
      SELECT
        '2000-01-01' :: TIMESTAMP AS system_created_at,
        address,
        meta,
        NAME
      FROM
        {{ source(
          'ethereum',
          'ethereum_contracts'
        ) }}
      UNION
      SELECT
        '2000-01-01' :: TIMESTAMP AS system_created_at,
        contract_address AS address,
        TO_OBJECT(PARSE_JSON(contract_meta)) AS meta,
        NAME
      FROM
        {{ source(
          'ethereum',
          'ethereum_contracts_backfill'
        ) }}
      WHERE
        CHECK_JSON(contract_meta) IS NULL
    )
  WHERE
    address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY LOWER(address)
  ORDER BY
    system_created_at DESC)) = 1)
  SELECT
    system_created_at,
    address,
    meta,
    NAME
  FROM
    base
  WHERE
    CASE
      WHEN meta :decimals :: STRING IS NOT NULL
      AND len(
        meta :decimals :: STRING
      ) >= 3 THEN TRUE
      ELSE FALSE
    END = FALSE
