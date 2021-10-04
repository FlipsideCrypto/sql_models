{{ config(
  materialized = 'incremental',
  unique_key = 'chain_id || block_id',
  incremental_strategy = 'delete+insert',
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
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ source(
      'terra_dbt',
      'blocks'
    ) }}
)
{% endif %}
)
SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
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
