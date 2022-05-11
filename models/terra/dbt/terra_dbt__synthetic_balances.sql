{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_timestamp, chain_id, block_id, inputs)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_balances']
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
     {{ source(
      'bronze',
      'prod_terra_sink_645110886'
    ) }}
  WHERE
    record_content:model:class in ('terra.balances.terra_synthetic_balances_model.TerraSyntheticBalancesModel',
    'terra.balances.terra_synthetic_balances_model.Terra5SyntheticBalancesModel')

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}

)

SELECT
  (record_metadata :CreateTime :: INT / 1000) :: TIMESTAMP AS _system_created_at,
  _inserted_timestamp,
  t.value :block_id :: bigint AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :chain_id :: STRING AS chain_id,
  t.value :function_name :: STRING AS function_name,
  t.value :inputs :: STRING AS inputs,
  t.value :project_id :: STRING AS project_id,
  t.value :project_name :: STRING AS project_name,
  t.value :value_numeric :: INTEGER AS value_numeric,
  t.value :value_obj :: OBJECT AS value_obj
FROM
  base_tables,
  LATERAL FLATTEN(input => record_content :results) t