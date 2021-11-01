{{ config(
  materialized = 'incremental',
  unique_key = 'chain_id || block_id || tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'terra_silver', 'terra_transactions']
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
      'terra_tx_model',
      'terra-5_tx_model'
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
  t.value :chain_id :: STRING AS chain_id,
  t.value :codespace :: STRING AS codespace,
  COALESCE(
    t.value :txhash :: STRING,
    -- Pre Columbus-5: tx_id
    -- Post Columbus-4: txhash
    t.value :tx_id :: STRING
  ) AS tx_id,
  t.value :tx_type :: STRING AS tx_type,
  t.value :tx_module :: STRING AS tx_module,
  t.value :tx_status :: STRING AS tx_status,
  t.value :tx_status_msg :: STRING AS tx_status_msg,
  t.value :tx_code :: INTEGER AS tx_code,
  t.value :fee :: ARRAY AS fee,
  t.value :gas_wanted :: DOUBLE AS gas_wanted,
  t.value :gas_used :: DOUBLE AS gas_used
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
