{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', date, address, currency, balance_type)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'silver_terra', 'silver_terra__synthetic_balances']
) }}

WITH dbt_balances AS(

SELECT
  *
FROM
  {{ ref('terra_dbt__synthetic_balances') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (SELECT DATEADD('day', -1, MAX(system_created_at :: DATE)) FROM {{ this }} AS balance

)

SELECT 
  block_id,
  block_timestamp,
  chain_id,
  SUBSTRING(inputs,25,44) as address,
  b.value:denom::string as currency,
  b.value:amount as balance
FROM dbt_balances,
LATERAL FLATTEN(input => value_obj :balances) b