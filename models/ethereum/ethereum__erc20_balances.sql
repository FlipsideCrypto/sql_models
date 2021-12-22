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
    DATE_TRUNC(
      'day',
      HOUR
    ) AS DAY,
    -- symbol,
    token_address,
    AVG(price) AS price
  FROM
    {{ ref('silver_ethereum__prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR >= getdate() - INTERVAL '2 days'
{% endif %}
GROUP BY
  DAY,
  token_address
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
