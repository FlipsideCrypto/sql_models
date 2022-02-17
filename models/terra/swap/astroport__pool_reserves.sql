{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, contract_address)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'astroport', 'terra', 'pool_reserves']
) }}

SELECT
  'terra' as blockchain,
  chain_id,
  block_id,
  block_timestamp,
  contract_address,
  total_share / pow(10,6) AS total_share,
  token_0_currency,
  token_0_amount / pow(10,6) AS token_0_amount,
  token_1_currency,
  token_1_amount / pow(10,6) AS token_1_amount
FROM
  {{ ref('silver_terra__astroport_pool_reserves') }}
