{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', date, address, currency, balance_type)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_flow', 'silver_flow__daily_balances']
) }}

WITH address_ranges AS (

  SELECT
    address,
    MIN(
      block_timestamp :: DATE
    ) AS min_block_date,
    MAX(
      CURRENT_TIMESTAMP :: DATE
    ) AS max_block_date
  FROM
    {{ source(
      'shared',
      'flow_balances'
    ) }}
  GROUP BY
    1
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
    A.address
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
    *
  FROM
    {{ source(
      'shared',
      'flow_balances'
    ) }}
    qualify(ROW_NUMBER() over(PARTITION BY address, currency, block_timestamp :: DATE
  ORDER BY
    block_timestamp DESC)) = 1
),
balance_tmp AS (
  SELECT
    DATE,
    d.address,
    currency,
    balance,
    balance_type,
    blockchain
  FROM
    all_dates d
    LEFT JOIN eth_balances b
    ON d.date = b.block_timestamp :: DATE
    AND d.address = b.address
)
SELECT
  DATE,
  address,
  LAST_VALUE(
    currency ignore nulls
  ) over(
    PARTITION BY address
    ORDER BY
      DATE ASC rows unbounded preceding
  ) AS currency,
  LAST_VALUE(
    balance ignore nulls
  ) over(
    PARTITION BY address
    ORDER BY
      DATE ASC rows unbounded preceding
  ) AS balance,
  LAST_VALUE(
    balance_type ignore nulls
  ) over(
    PARTITION BY address
    ORDER BY
      DATE ASC rows unbounded preceding
  ) AS balance_type,
  LAST_VALUE(
    blockchain ignore nulls
  ) over(
    PARTITION BY address
    ORDER BY
      DATE ASC rows unbounded preceding
  ) AS blockchain
FROM
  balance_tmp
