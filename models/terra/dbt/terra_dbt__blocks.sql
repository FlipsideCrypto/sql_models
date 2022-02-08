{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_blocks']
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
      'terra_block_model',
      'terra-5_block_model'
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
  record_content :model :blockchain :: STRING AS chain_id,
  t.value :block_id :: INT AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :blockchain :: STRING AS blockchain,
  t.value :proposer_address :: STRING AS proposer_address
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
