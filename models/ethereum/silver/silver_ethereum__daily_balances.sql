{{ config(
  materialized = 'incremental',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'silver_ethereum', 'silver_ethereum__daily_balances']
) }}

with latest as (
select date,
address,
contract_address,
balance
from {{ this }}
where date = (select dateadd('day',-2,max(date)) from {{ this }})
), 
new as (
select block_timestamp::date as date,
address,
contract_address,
balance,
1 as rank
from {{ ref('silver_ethereum__balances') }}
where block_timestamp::date >= (select dateadd('day',-2,max(date)) from {{ this }})
qualify(row_number() over(partition by address, contract_address, block_timestamp::date order by block_timestamp desc)) = 1
), 
incremental as (
select date,
address,
contract_address,
balance
from (
select date,
address,
contract_address,
balance,
2 as rank
from latest

union

select date,
address,
contract_address,
balance,
1 as rank
from new
)
qualify(row_number() over(partition by address, contract_address, date order by rank asc)) = 1
),
base_balances as (
{% if is_incremental() %}
select date as block_timestamp,
address,
contract_address,
balance
from incremental
{% else %}
select block_timestamp
address,
contract_address
{{ ref('silver_ethereum__balances') }}
{% endif %}
),
address_ranges AS (

  SELECT
    address,
    contract_address,
    'ethereum' AS blockchain,
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
    3
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
    A.contract_address,
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
    contract_address,
    block_timestamp,
    'ethereum' AS blockchain,
    balance
  FROM
    base_balances
    qualify(ROW_NUMBER() over(PARTITION BY address, contract_address, block_timestamp :: DATE
  ORDER BY
    balance DESC)) = 1
),
balance_tmp AS (
  SELECT
    d.date,
    d.address,
    d.contract_address,
    b.balance,
    d.blockchain
  FROM
    all_dates d
    LEFT JOIN eth_balances b
    ON d.date = b.block_timestamp :: DATE
    AND d.address = b.address
    AND d.contract_address = b.contract_address
    AND d.blockchain = b.blockchain
)
SELECT
  DATE,
  address,
  contract_address,
  blockchain,
  LAST_VALUE(
    balance ignore nulls
  ) over(
    PARTITION BY address,
    contract_address,
    blockchain
    ORDER BY
      DATE ASC rows unbounded preceding
  ) AS balance
FROM
  balance_tmp
