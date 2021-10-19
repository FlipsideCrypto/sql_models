{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id || nf_token_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'uniswapv3_silver', 'uniswapv3_dbt__positions']
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
    {{ source(
      'bronze',
      'prod_ethereum_sink_407559501'
    ) }}
  WHERE
    record_content :model :name :: STRING IN (
      'uniswap_v3_mainnet_position_model'
    )

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
   (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  t.value :blockchain :: STRING AS blockchain,
  t.value :block_id :: bigint AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :fee :: INTEGER AS fee,
  t.value :fee_growth_inside0_last_x128 :: FLOAT AS fee_growth_inside0_last_x128,
  t.value :fee_growth_inside1_last_x128 :: FLOAT AS fee_growth_inside1_last_x128,
  t.value :is_active :: BOOLEAN AS is_active,
  t.value :liquidity :: FLOAT AS liquidity,
  t.value :liquidity_adjusted :: FLOAT AS liquidity_adjusted,
  t.value :liquidity_provider :: STRING AS liquidity_provider,
  t.value :nf_position_manager_address :: STRING AS nf_position_manager_address,
  t.value :nf_token_id :: INTEGER AS nf_token_id,
  t.value :pool_address :: STRING AS pool_address,
  t.value :tick_lower :: INTEGER AS tick_lower,
  t.value :tick_upper :: INTEGER AS tick_upper,
  t.value :tokens_owed0 :: FLOAT AS tokens_owed0,
  t.value :tokens_owed1 :: FLOAT AS tokens_owed1,
  t.value :tx_id :: STRING AS tx_id
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
