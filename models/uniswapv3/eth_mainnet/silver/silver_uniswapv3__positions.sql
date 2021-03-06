{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, block_id, COALESCE(nf_token_id, -1))",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'uniswapv3_silver', 'silver_uniswapv3__positions']
) }}

WITH silver AS (

  SELECT
    system_created_at,
    blockchain,
    block_id,
    block_timestamp,
    fee,
    fee_growth_inside0_last_x128,
    fee_growth_inside1_last_x128,
    is_active,
    liquidity,
    liquidity_adjusted,
    liquidity_provider,
    nf_position_manager_address,
    nf_token_id,
    pool_address,
    tick_lower,
    tick_upper,
    tokens_owed0,
    tokens_owed1,
    tx_id
  FROM
    {{ ref('uniswapv3_dbt__positions') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS positions
)
{% endif %}
UNION ALL
SELECT
  '2000-01-01' :: TIMESTAMP AS system_created_at,
  blockchain,
  block_id,
  block_timestamp,
  fee,
  fee_growth_inside0_last_x128,
  fee_growth_inside1_last_x128,
  is_active,
  liquidity,
  liquidity_adjusted,
  liquidity_provider,
  nf_position_manager_address,
  nf_token_id,
  pool_address,
  tick_lower,
  tick_upper,
  tokens_owed0,
  tokens_owed1,
  tx_id
FROM
  {{ source(
    'uniswapv3_eth',
    'uniswapv3_positions'
  ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(block_timestamp :: DATE))
  FROM
    {{ this }} AS positions
)
{% endif %}
)
SELECT
  system_created_at,
  blockchain,
  block_id,
  block_timestamp,
  COALESCE(fee, 0) as fee,
  COALESCE(fee_growth_inside0_last_x128, 0) as fee_growth_inside0_last_x128,
  COALESCE(fee_growth_inside1_last_x128, 0) as fee_growth_inside1_last_x128,
  is_active,
  COALESCE(liquidity, 0) as liquidity,
  COALESCE(liquidity_adjusted, 0) as liquidity_adjusted,
  liquidity_provider,
  nf_position_manager_address,
  nf_token_id,
  pool_address,
  COALESCE(tick_lower, 0) as tick_lower,
  COALESCE(tick_upper, 0) as tick_upper,
  COALESCE(tokens_owed0, 0) as tokens_owed0,
  COALESCE(tokens_owed1, 0) as tokens_owed1,
  tx_id
FROM
  silver qualify(ROW_NUMBER() over(PARTITION BY tx_id, nf_token_id
ORDER BY
  system_created_at DESC)) = 1
