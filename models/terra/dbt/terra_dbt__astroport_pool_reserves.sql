{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, contract_address)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'astroport', 'pool_reserves']
) }}

select
    (
      record_metadata :CreateTime :: INT / 1000
    ) :: TIMESTAMP AS system_created_at,
    r.value:chain_id :: STRING as chain_id,
    r.value:block_id :: INT as block_id,
    r.value:blockchain :: STRING as blockchain,
    r.value:block_timestamp :: TIMESTAMP as block_timestamp,
    r.value:contract_address :: STRING as contract_address,
    r.value:value_obj:total_share :: INTEGER as total_share,
    r.value:value_obj:asset_1 :: STRING as token_0_currency,
    r.value:value_obj:asset_1_value :: INTEGER as token_0_amount,
    r.value:value_obj:asset_2 :: STRING as token_1_currency,
    r.value:value_obj:asset_2_value :: INTEGER as token_1_amount
from
    {{ source(
      'bronze',
      'prod_terra_sink_645110886'
    ) }},
    lateral flatten(input => record_content:results) as r
where
    record_content:model.name = 'terra-5_astroport_pool_reserves'
    and record_content:model.run_id = 'v2022.02.14.0'
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
order by block_id asc