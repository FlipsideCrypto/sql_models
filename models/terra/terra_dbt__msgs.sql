{{ config(
  materialized = 'incremental',
  unique_key = 'chain_id || block_id || tx_id || msg_index',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'terra_silver', 'terra_msgs']
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
    record_content :model :name :: STRING IN ('terra_msg_model', '')

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ source(
      'terra_dbt',
      'msgs'
    ) }}
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
  t.value :chain_id :: STRING AS chain_id,
  t.value :tx_id :: STRING AS tx_id,
  t.value :tx_type :: STRING AS tx_type,
  t.value :tx_status :: STRING AS tx_status,
  t.value :tx_module :: STRING AS tx_module,
  t.value :msg_index :: INTEGER AS msg_index,
  t.value :msg_type :: STRING AS msg_type,
  t.value :msg_module :: STRING AS msg_module,
  t.value :msg_value :: variant AS msg_value
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
