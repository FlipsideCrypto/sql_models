{{ config(
  materialized = 'incremental',
  sort = 'hour',
  unique_key = 'hour',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'events']
) }}

WITH full_decimals AS (

  SELECT
    LOWER(address) AS contract_address,
    meta :decimals :: INT AS decimals,
    meta :symbol :: STRING AS symbol
  FROM
    {{ ref('silver_ethereum__contracts') }}
  WHERE
    meta :decimals NOT LIKE '%00%' qualify(ROW_NUMBER() over(PARTITION BY contract_address
  ORDER BY
    decimals DESC) = 1) --need the %00% filter to exclude messy data
),
hourly_prices AS (
  SELECT
    p.symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    LOWER(
      A.token_address
    ) AS token_address,
    d.decimals,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'prices_v2'
    ) }}
    p
    LEFT OUTER JOIN {{ source(
      'shared',
      'market_asset_metadata'
    ) }} A
    ON p.asset_id = A.asset_id
    LEFT OUTER JOIN full_decimals d
    ON d.contract_address = LOWER(
      A.token_address
    )
  WHERE
    (
      A.platform_id = '1027'
      OR A.asset_id = '1027'
      OR A.platform_id = 'ethereum'
    )

{% if is_incremental() %}
AND recorded_at >= CURRENT_DATE - 3
{% else %}
  AND recorded_at >= '2020-05-05' -- first date with valid prices data
{% endif %}
GROUP BY
  1,
  2,
  3,
  4
),
token_addresses AS (
  SELECT
    DISTINCT LOWER(
      A.token_address
    ) AS token_address
  FROM
    {{ source(
      'shared',
      'prices_v2'
    ) }}
    p
    LEFT OUTER JOIN {{ source(
      'shared',
      'market_asset_metadata'
    ) }} A
    ON p.asset_id = A.asset_id
  WHERE
    (
      A.platform_id = '1027'
      OR A.asset_id = '1027'
      OR A.platform_id = 'ethereum'
    )
    AND (
      token_address IS NOT NULL
      OR p.symbol = 'ETH'
    )
),
hour_token_addresses_pair AS (
  SELECT
    *
  FROM
    silver.hours
    CROSS JOIN token_addresses

{% if is_incremental() %}
WHERE
  HOUR BETWEEN CURRENT_DATE - 3
  AND DATE_TRUNC('hour', SYSDATE())
{% else %}
WHERE
  HOUR BETWEEN '2020-05-05'
  AND DATE_TRUNC('hour', SYSDATE()) -- first date with valid prices data
{% endif %}),
imputed AS (
  SELECT
    h.hour,
    h.token_address,
    p.symbol,
    p.decimals,
    p.price AS avg_price,
    LAG(
      p.symbol
    ) ignore nulls over (
      PARTITION BY h.token_address
      ORDER BY
        h.hour
    ) AS lag_symbol,
    LAG(
      p.decimals
    ) ignore nulls over (
      PARTITION BY h.token_address
      ORDER BY
        h.hour
    ) AS lag_decimals,
    LAG(
      p.price
    ) ignore nulls over (
      PARTITION BY h.token_address
      ORDER BY
        h.hour
    ) AS imputed_price
  FROM
    hour_token_addresses_pair h
    LEFT OUTER JOIN hourly_prices p
    ON p.hour = h.hour
    AND (
      p.token_address = h.token_address
      OR (
        h.token_address IS NULL
        AND p.symbol = 'ETH'
      )
    )
),
FINAL AS (
  SELECT
    p.hour AS HOUR,
    p.token_address,
    -- CASE
    --   WHEN symbol IS NOT NULL THEN symbol
    --   ELSE lag_symbol
    -- END AS symbol,
    CASE
      WHEN decimals IS NOT NULL THEN decimals
      ELSE lag_decimals
    END AS decimals,
    CASE
      WHEN avg_price IS NOT NULL THEN avg_price
      ELSE imputed_price
    END AS price,
    CASE
      WHEN avg_price IS NULL THEN TRUE
      ELSE FALSE
    END AS is_imputed
  FROM
    imputed p
  WHERE
    price IS NOT NULL
)
SELECT
  f.HOUR,
  f.token_address,
  d.symbol,
  f.decimals,
  f.price,
  f.is_imputed
FROM
  FINAL f
left outer join full_decimals d on d.contract_address = f.token_address
qualify(ROW_NUMBER() over(PARTITION BY f.HOUR, f.token_address, d.symbol
ORDER BY
  f.decimals DESC) = 1)
