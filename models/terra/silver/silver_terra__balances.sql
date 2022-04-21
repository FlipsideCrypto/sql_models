{{ config(
  materialized = 'incremental',
  unique_key = 'block_timestamp::date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::date'],
  tags = ['snowflake', 'silver_terra', 'balances']
) }}



WITH tbl AS (
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
    FROM {{ ref('terra_dbt__balances')}}
    WHERE 1=1
    AND _inserted_timestamp >= (
        SELECT MAX(_inserted_timestamp)
        FROM {{ this }}
    )
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
      case when block_number <= 3820000 then 'columbus-3'
           else 'columbus-4' end AS blockchain,
      currency,
      '2000-01-01 00:00:00'::timestamp as system_created_at,
      '2000-01-01 00:00:00'::timestamp as _inserted_timestamp
    FROM {{ source('shared', 'terra_balances') }}
  {% endif %}
)
SELECT * FROM tbl
-- WHERE address != 'terra1fl48vsnmsdzcv85q5d2q4z5ajdha8yu3nln0mh'
qualify(ROW_NUMBER() over(PARTITION BY address, balance_type, blockchain, block_number, currency
ORDER BY _inserted_timestamp DESC)) = 1