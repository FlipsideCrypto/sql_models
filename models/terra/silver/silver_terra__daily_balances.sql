{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', date, address, currency, balance_type)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'silver_terra', 'silver_terra__daily_balances']
) }}

WITH address_ranges AS (

  SELECT
    address,
    currency,
    balance_type,
    'terra' AS blockchain,
    MIN(
      block_timestamp :: DATE
    ) AS min_block_date,
    MAX(
      CURRENT_TIMESTAMP :: DATE
    ) AS max_block_date
  FROM
    {{ source(
      'shared',
      'terra_balances'
    ) }}
  GROUP BY
    1,
    2,
    3,
    4
),
cte_my_date AS (
  SELECT
    HOUR :: DATE AS DATE
  FROM
    {{ source(
      'shared',
      'hours'
    ) }}
  GROUP BY
    1
),
all_dates AS (
  SELECT
    C.date,
    A.address,
    A.currency,
    A.balance_type,
    A.blockchain
  FROM
    cte_my_date C
    LEFT JOIN address_ranges A
    ON C.date BETWEEN A.min_block_date
    AND A.max_block_date
  WHERE
    A.address IS NOT NULL
),
eth_balances AS (
  SELECT
    address,
    currency,
    block_timestamp,
    balance_type,
    'terra' AS blockchain,
    balance
  FROM
    {{ source(
      'shared',
      'terra_balances'
    ) }}
    qualify(ROW_NUMBER() over(PARTITION BY address, currency, block_timestamp :: DATE, balance_type
  ORDER BY
    balance DESC)) = 1
),
balance_tmp AS (
  SELECT
    d.date,
    d.address,
    d.currency,
    b.balance,
    d.balance_type,
    d.blockchain
  FROM
    all_dates d
    LEFT JOIN eth_balances b
    ON d.date = b.block_timestamp :: DATE
    AND d.address = b.address
    AND d.currency = b.currency
    AND d.balance_type = b.balance_type
    AND d.blockchain = b.blockchain
)
SELECT
  DATE,
  address,
  currency,
  balance_type,
  blockchain,
  LAST_VALUE(
    balance ignore nulls
  ) over(
    PARTITION BY address,
    currency,
    balance_type,
    blockchain
    ORDER BY
      DATE ASC rows unbounded preceding
  ) AS balance
FROM
  balance_tmp
