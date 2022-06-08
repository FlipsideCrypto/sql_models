{{ config(
  materialized = 'incremental',
  unique_key = 'address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['address'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__contracts']
) }}

WITH dbt_model AS (

  SELECT
    system_created_at,
    address,
    meta,
    NAME
  FROM
    {{ ref('ethereum_dbt__contracts') }}
  WHERE
    meta IS NOT NULL
    AND address IS NOT NULL
),
legacy_model AS (
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
  WHERE
    meta IS NOT NULL
    AND address IS NOT NULL
),
backfill AS (
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
),
clean_backfill AS (
  SELECT
    *
  FROM
    backfill
  WHERE
    GET(
      meta,
      'decimals'
    ) IS NOT NULL
),
union_models AS (
  SELECT
    system_created_at,
    address,
    meta,
    NAME,
    'dbt' AS model
  FROM
    dbt_model
  UNION ALL
  SELECT
    system_created_at,
    address,
    meta,
    NAME,
    'legacy' AS model
  FROM
    legacy_model
  UNION ALL
  SELECT
    system_created_at,
    address,
    meta,
    NAME,
    'backfill' AS model
  FROM
    clean_backfill
),
base AS (
  SELECT
    *
  FROM
    union_models
  WHERE
    address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY LOWER(address)
  ORDER BY
    system_created_at DESC, model ASC)) = 1)
  SELECT
    system_created_at,
    LOWER(address) AS address,
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
