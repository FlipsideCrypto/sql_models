{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_timestamp, address, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'silver_terra', 'silver_terra__synthetic_balances']
) }}


with dedupe as (

SELECT
  system_created_at,
  block_id,
  block_timestamp,
  chain_id,
  SUBSTRING(inputs,25,44) as address,
  b.value:denom::string as currency,
  'liquid' as balance_type,
  false as is_native,
  b.value:amount::FLOAT as balance,
  row_number() OVER (PARTITION BY date_trunc('day', block_timestamp), address, currency, chain_id ORDER BY block_timestamp DESC, block_id DESC) as rn
FROM {{ ref('terra_dbt__synthetic_balances') }},
LATERAL FLATTEN(input => value_obj :balances) b
WHERE 1 = 1
)

SELECT * FROM dedupe
WHERE rn = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= ( SELECT DATEADD('day', -1, MAX(system_created_at :: DATE)) FROM {{ this }})
{% endif %}