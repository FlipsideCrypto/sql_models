{{ config(
  materialized = 'incremental',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'silver_terra', 'balances']
) }}


-- Qualify the row number here 
{% if is_incremental() %}
  SELECT 
    address,
    balance,
    balance_type,
    block_number,
    block_timestamp,
    blockchain,
    currency
  FROM {{ ref('terra_dbt__balances')}}
  WHERE 1=1
  AND _inserted_timestamp >= (
      SELECT MAX(_inserted_timestamp)
      FROM {{ this }}
  )
  qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
  ORDER BY
    system_created_at DESC)) = 1
{% else %}
  SELECT 
    address,
    balance,
    balance_type,
    block_number,
    block_timestamp,
    blockchain,
    currency
  FROM {{ ref('terra_dbt__balances')}}
  qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
  ORDER BY
    system_created_at DESC)) = 1

  UNION ALL 

  SELECT 
    address,
    balance,
    balance_type,
    block_number,
    block_timestamp,
    'columbus-3' AS blockchain,
    currency
  FROM {{ source('shared', 'terra_balances') }}
  WHERE date(block_timestamp) < '2020-10-04' AND block_number <= 3820000
{% endif %}



