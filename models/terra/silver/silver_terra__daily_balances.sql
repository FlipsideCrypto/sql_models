{{ config(
  materialized = 'incremental',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'silver_terra', 'silver_terra__daily_balances']
) }}

with latest as (
select DATE,
  address,
  currency,
  balance_type,
  blockchain,
  CASE
    WHEN currency IN ('LUNA', 'UST', 'KRT', 'MNT', 'SDT', 'krw', 'mnt', 'sdr') THEN balance
    ELSE balance * POW(10,6)
    END AS balance
from {{ this }}
where date = (select dateadd('day',-1,max(date)) from {{ this }})
), 
new as (
select block_timestamp::date as date,
address,
currency,
balance,
blockchain,
balance_type,
1 as rank
from {{ source(
      'shared',
      'terra_balances'
    ) }}
where block_timestamp::date >= (select dateadd('day',-1,max(date)) from {{ this }})
qualify(row_number() over(partition by address, currency,blockchain, balance_type,block_timestamp::date order by block_timestamp desc)) = 1
), 
incremental as (
select DATE,
  address,
  currency,
  balance_type,
  blockchain,
  balance
from (
select DATE,
  address,
  currency,
  balance_type,
  blockchain,
  balance,
2 as rank
from latest

union

select DATE,
  address,
  currency,
  balance_type,
  blockchain,
  balance,
1 as rank
from new
)
qualify(row_number() over(partition by address, currency,blockchain, balance_type, date order by rank asc)) = 1
),
base_balances as (
{% if is_incremental() %}
select date as block_timestamp,
  address,
  currency,
  balance_type,
  blockchain,
  balance
from incremental
{% else %}
select block_timestamp,
  address,
  currency,
  balance_type,
  blockchain,
  balance
from {{source(
      'shared',
      'terra_balances'
    ) }}
{% endif %}
),
 address_ranges AS (

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
    base_balances
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
terra_balances AS (
  SELECT
    address,
    currency,
    block_timestamp,
    balance_type,
    'terra' AS blockchain,
    balance
  FROM
    base_balances
    qualify(ROW_NUMBER() over(PARTITION BY address, currency, block_timestamp :: DATE, balance_type
  ORDER BY
    balance DESC)) = 1
),
balance_tmp AS (
  SELECT
    d.date,
    d.address,
    d.currency,
    CASE
    WHEN d.currency IN ('LUNA', 'UST', 'KRT', 'MNT', 'SDT', 'krw', 'mnt', 'sdr') THEN b.balance
    ELSE b.balance / POW(10,6)
    END AS balance,
    d.balance_type,
    d.blockchain
  FROM
    all_dates d
    LEFT JOIN terra_balances b
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
  TRUE as is_native,
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
