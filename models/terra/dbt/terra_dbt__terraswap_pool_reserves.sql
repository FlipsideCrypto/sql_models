{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, contract_address)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'terraswap', 'pool_reserves']
) }}

SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  r.value :chain_id :: STRING AS chain_id,
  r.value :block_id :: INT AS block_id,
  r.value :blockchain :: STRING AS blockchain,
  r.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  r.value :contract_address :: STRING AS contract_address,
  r.value :value_obj :total_share :: INTEGER AS total_share,
  r.value :value_obj :asset_1 :: STRING AS token_0_currency,
  r.value :value_obj :asset_1_value :: INTEGER AS token_0_amount,
  r.value :value_obj :asset_2 :: STRING AS token_1_currency,
  r.value :value_obj :asset_2_value :: INTEGER AS token_1_amount
FROM
  {{ source(
    'bronze',
    'prod_terra_sink_645110886'
  ) }},
  LATERAL FLATTEN(
    input => record_content :results
  ) AS r
WHERE
  (
    record_content :model.name = 'terra-5_terraswap_pool_reserves'
    OR record_content :model.name = 'terra-5_terraswap_pool_reserves_bison'
    OR record_content :model.name = 'terra-5_terraswap_pool_reserves_backfill'
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
