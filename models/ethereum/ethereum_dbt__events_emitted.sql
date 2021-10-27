{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_id || event_index',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_ethereum', 'ethereum_dbt__events_emitted']
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
    {{ source(
      'bronze',
      'prod_ethereum_sink_407559501'
    ) }}
  WHERE
    record_content :model :name :: STRING IN (
      'eth_events_emitted_model'
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
  t.value :block_id :: bigint AS block_id,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :contract_address :: STRING AS contract_addr,
  t.value :contract_name :: STRING AS contract_name,
  t.value :event_index :: INTEGER AS event_index,
  t.value :event_inputs :: OBJECT AS event_inputs,
  t.value :event_name :: STRING AS event_name,
  t.value :event_removed :: BOOLEAN AS event_removed,
  t.value :tx_from :: STRING AS tx_from_addr,
  t.value :tx_id :: STRING AS tx_id,
  t.value :tx_succeeded :: BOOLEAN AS tx_succeeded,
  t.value :tx_to :: STRING AS tx_to_addr
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t