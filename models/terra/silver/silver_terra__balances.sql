{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', address, balance_type, block_number, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::date'],
  tags = ['snowflake', 'silver_terra', 'balances']
) }}

WITH inc as (
  SELECT 
      address,
      balance,
      balance_type,
      block_number,
      block_timestamp,
      blockchain,
      currency,
      system_created_at,
      _inserted_timestamp
    FROM {{ ref('terra_dbt__balances')}}
    WHERE 1=1
    AND _inserted_timestamp >= (
        SELECT MAX(_inserted_timestamp)
        FROM {{ this }}
    )
  ),
  tbl AS (
  {% if is_incremental() %}
    SELECT 
      address,
      balance,
      balance_type,
      block_number,
      block_timestamp,
      blockchain,
      currency,
      system_created_at,
      _inserted_timestamp
    FROM inc

    UNION

    select address,
    balance,
    balance_type,
    block_number,
    block_timestamp,
    blockchain,
    currency,
    system_created_at,
    _inserted_timestamp
    from silver_terra.latest_balances
    where address in (select distinct address from inc)
  {% else %}
    SELECT 
      address,
      balance,
      balance_type,
      block_number,
      block_timestamp,
      blockchain,
      currency,
      system_created_at,
      _inserted_timestamp
    FROM {{ ref('terra_dbt__balances')}}

    UNION ALL 

    SELECT 
      address,
      balance,
      balance_type,
      block_number,
      block_timestamp,
      case when block_number <= 3820000 and block_timestamp < '2020-10-03 15:56:10.000' then 'columbus-3'
           when blockchain = 'terra-5' then 'columbus-5'
           else 'columbus-4' end AS blockchain,
      currency,
      '2000-01-01 00:00:00'::timestamp as system_created_at,
      '2000-01-01 00:00:00'::timestamp as _inserted_timestamp
    FROM {{ source('shared', 'terra_balances') }}
    where balance <> 0
  {% endif %}
),
tmp as (
SELECT * 
FROM tbl
-- WHERE address != 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
qualify(ROW_NUMBER() over(PARTITION BY address, balance_type, blockchain, block_number, currency
ORDER BY _inserted_timestamp DESC)) = 1
), dense_tmp as (
select *,
dense_rank() over(partition by address, currency, balance_type order by block_number asc) as rn
from tmp
), joined as (
select t.address as t_address,
t.balance as t_balance,
t.balance_type as t_balance_type,
t.block_number as t_block_number,
t.block_timestamp as t_block_timestamp,
t.blockchain as t_blockchain,
t.currency as t_currency,
t.system_created_at as t_system_created_at,
t._inserted_timestamp as t_inserted_timestamp,
t2.address as t2_address,
t2.balance as t2_balance,
t2.balance_type as t2_balance_type,
t2.block_number as t2_block_number,
t2.block_timestamp as t2_block_timestamp,
t2.blockchain as t2_blockchain,
t2.currency as t2_currency,
t2.system_created_at as t2_system_created_at,
t2._inserted_timestamp as t2_inserted_timestamp,
  LAST_VALUE(
            t2.block_timestamp
        IGNORE NULLS
        ) over (
            PARTITION BY coalesce(t.address,t2.address), t.rn
            ORDER BY
                t2.block_timestamp
        ) AS block_timestamp,
  LAST_VALUE(
            t2.block_number
        IGNORE NULLS
        ) over (
            PARTITION BY coalesce(t.address,t2.address), t.rn
            ORDER BY
                t2.block_timestamp
        ) AS block_number,
  t.rn as t_rn,
  t2.rn as t2_rn,
  max(t2.rn) over(partition by coalesce(t.address,t2.address), coalesce(t.balance_type,t2.balance_type), coalesce(t.currency,t2.currency)) as max_t2_rn
from dense_tmp t
full outer join dense_tmp t2 on t.address = t2.address
                      and t.balance_type = t2.balance_type
                      and t.currency = t2.currency
                      and t.rn = t2.rn - 1
)

select coalesce(t2_address, t_address) as address,
coalesce(t2_balance, 0) as balance,
coalesce(t2_balance_type, t_balance_type) as balance_type,
coalesce(t2_currency, t_currency) as currency,
coalesce(t2_blockchain, t_blockchain) as blockchain,
coalesce(t2_block_number, block_number) as block_number,
coalesce(t2_block_timestamp, block_timestamp) as block_timestamp,
coalesce(t2_inserted_timestamp, t_inserted_timestamp) as _inserted_timestamp,
coalesce(t2_system_created_at, t_system_created_at) as system_created_at
from joined
where coalesce(t_rn,0) <> max_t2_rn
and currency is not null