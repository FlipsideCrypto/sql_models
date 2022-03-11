{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', date, address, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'terra_silver', 'silver_terra__synthetic_balances']
) }}

WITH address_ranges AS (

  SELECT
    SUBSTRING(inputs,25,44) as address,
    b.value:denom::string as currency,
    'liquid' as balance_type,
    'terra' AS blockchain,
    MIN(
      block_timestamp :: DATE
    ) AS min_block_date,
    MAX(
      CURRENT_TIMESTAMP :: DATE
    ) AS max_block_date
  FROM {{ ref('terra_dbt__synthetic_balances') }},
  LATERAL FLATTEN(input => value_obj :balances) b
  GROUP BY address, currency, balance_type, blockchain
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

synth_balances AS (
  SELECT
    SUBSTRING(inputs,25,44) AS address,
    b.value:denom::string AS currency,
    block_timestamp,
    'liquid' AS balance_type,
    'terra' AS blockchain,
    b.value:amount::FLOAT as balance
  FROM
    {{ ref('terra_dbt__synthetic_balances') }},
    LATERAL FLATTEN(input => value_obj :balances) b

    qualify(ROW_NUMBER() over(PARTITION BY address, currency, block_timestamp :: DATE, balance_type
  ORDER BY
    balance DESC)) = 1
),

synth_balance_tmp AS (
  SELECT
    d.date,
    d.address,
    d.currency,
    b.balance,
    d.balance_type,
    d.blockchain
  FROM
    all_dates d
    LEFT JOIN synth_balances b
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
  FALSE AS is_native,
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
  synth_balance_tmp
