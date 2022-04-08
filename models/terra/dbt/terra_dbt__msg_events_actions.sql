{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, msg_index, event_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_msg_events_actions']
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
      'terra_msg_event_model',
      'terra-5_msg_event_model'
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
  COALESCE(
    t.value :txhash :: STRING,
    -- Pre Columbus-5: tx_id
    -- Post Columbus-4: txhash
    t.value :tx_id :: STRING
  ) AS tx_id,
  t.value :tx_module :: STRING AS tx_module,
  t.value :tx_status :: STRING AS tx_status,
  t.value :tx_type :: STRING AS tx_type,
  t.value :msg_index :: INTEGER AS msg_index,
  t.value :msg_type :: STRING AS msg_type,
  t.value :msg_module :: STRING AS msg_module,
  t.value :event_type :: STRING AS event_type,
  t.value :event_index :: INTEGER AS event_index,
  t.value :event_attributes :: OBJECT AS event_attributes,
  t.value :event_attributes_actions :: OBJECT AS event_attributes_actions
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
WHERE t.value :event_attributes_actions :: OBJECT IS NOT NULL
