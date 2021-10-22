{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['tx_id'],
  tags = ['snowflake', 'ethereum', 'silver_ethereum','silver_ethereum__proxy_contract_registry']
) }}


SELECT
  system_created_at,
  block_id,
  block_timestamp,
  logic_contract_address,
  proxy_contract_address,
  tx_id
FROM
  {{ ref('ethereum_dbt__proxy_contract_registry') }}

union

SELECT
  '2000-01-01'::timestamp as system_created_at,
  block_id,
  block_timestamp,
  logic_contract_address,
  proxy_contract_address,
  tx_id
FROM
  {{ source('ethereum','ethereum_proxy_contract_registry') }}
qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  system_created_at DESC)) = 1