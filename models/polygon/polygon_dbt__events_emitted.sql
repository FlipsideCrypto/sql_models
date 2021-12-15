{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id, event_index)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'polygon_silver', 'polygon_dbt_events_emitted','polygon']
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
    {{ source(
      'bronze',
      'prod_matic_sink_510901820'
    ) }}
  WHERE
    record_content :model :name :: STRING = 'polygon_events_emitted_model'

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
  record_content :model :blockchain :: STRING AS chain_id,
  A.value :block_id :: INT AS block_id,
  A.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  A.value :contract_address :: STRING AS contract_address,
  A.value :contract_name :: STRING AS contract_name,
  A.value :event_index :: INT AS event_index,
  A.value :event_inputs :: OBJECT AS event_inputs,
  A.value :event_name :: STRING AS event_name,
  A.value :event_removed :: BOOLEAN AS event_removed,
  A.value :tx_from :: STRING AS tx_from,
  A.value :tx_id :: STRING AS tx_id,
  A.value :tx_succeeded :: BOOLEAN AS tx_succeeded,
  A.value :tx_to :: STRING AS tx_to
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) A
