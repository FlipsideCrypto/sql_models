{{ config(
  materialized = 'incremental',
  sort = ['balance_date', 'symbol'],
  unique_key = 'balance_date || user_address',
  incremental_strategy = 'delete+insert',
  cluster_by = ['balance_date', 'user_address', 'contract_address'],
  tags = ['snowflake', 'ethereum', 'balances', 'erc20_balances', 'address_labels']
) }}
-- Get the average token price per day
WITH token_prices AS (

  SELECT
    p.symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    LOWER(
      A.token_address
    ) AS token_address,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'prices_v2'
    ) }}
    p
    JOIN {{ source(
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

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '2 days'
{% else %}
{% endif %}
GROUP BY
  1,
  2,
  3
),
balances AS (
  SELECT
    balance_date,
    b.address AS user_address,
    labels.project_name AS label,
    labels.address_name AS address_name,
    labels.l1_label AS label_type,
    labels.l2_label AS label_subtype,
    b.contract_address,
    contract_labels.project_name AS contract_label,
    b.symbol,
    token_prices.price,
    b.balance,
    -- Value of the token in USD
    token_prices.price * balance AS amount_usd
  FROM
    -- join against the clean daily balances table
    (
      {{ safe_ethereum_balances(
        source(
          'ethereum',
          'daily_ethereum_token_balances'
        ),
        '1 days',
        '9 months'
      ) }}
    ) b
    LEFT OUTER JOIN -- Labels for addresses
    {{ ref('silver_crosschain__address_labels') }} AS labels
    ON b.address = labels.address
    AND labels.blockchain = 'ethereum'
    AND labels.creator = 'flipside'
    LEFT OUTER JOIN -- Labels for contracts
    {{ ref('silver_crosschain__address_labels') }} AS contract_labels
    ON contract_labels.address = b.contract_address
    AND contract_labels.blockchain = 'ethereum'
    AND contract_labels.creator = 'flipside'
    LEFT OUTER JOIN token_prices
    ON token_prices.token_address = b.contract_address
    AND DATE_TRUNC(
      'day',
      b.balance_date
    ) = token_prices.day
  WHERE

{% if is_incremental() %}
balance_date >= getdate() - INTERVAL '1 days'
{% else %}
  balance_date >= getdate() - INTERVAL '9 months'
{% endif %}
)
SELECT
  *
FROM
  balances
ORDER BY
  balance_date DESC
