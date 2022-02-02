{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, transition_type, index, event)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_transitions']
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
    record_content :model :name :: STRING IN (
      'terra_transition_model',
      'terra-5_transition_model'
    )

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
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  _inserted_timestamp,
  t.value :blockchain :: STRING AS blockchain,
  t.value :block_id :: bigint AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :chain_id :: STRING AS chain_id,
  t.value :event :: STRING AS event,
  t.value :index :: INTEGER AS INDEX,
  t.value :transition_type :: STRING AS transition_type,
  t.value :attributes :: OBJECT AS event_attributes
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
