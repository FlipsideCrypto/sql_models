{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', address, balance_type, block_number, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::date'],
  tags = ['snowflake', 'silver_terra', 'balances']
) }}

with max_bn as (
select address,
  max(block_timestamp) as block_timestamp
from {{ ref('silver_terra__balances')}}
  group by 1
)
select b.address,
b.balance,
b.balance_type,
b.block_number,
b.block_timestamp,
b.blockchain,
b.currency,
b.system_created_at,
b._inserted_timestamp
from {{ ref('silver_terra__balances')}} b
join max_bn m on b.address = m.address 
             and b.block_timestamp = m.block_timestamp
