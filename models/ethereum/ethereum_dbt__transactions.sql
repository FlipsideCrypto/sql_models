{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_hash',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_ethereum', 'ethereum_dbt__transactions']
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
      'udm_transactions'
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
  t.value :tx_hash :: STRING AS tx_hash,
  t.value :tx_position :: INTEGER AS tx_position,
  t.value :nonce :: INTEGER AS nonce,
  t.value :from_address :: STRING AS from_address,
  t.value :to_address :: STRING AS to_address,
  t.value :input_method :: STRING AS input_method,
  t.value :gas_price :: FLOAT AS gas_price,
  t.value :gas_limit :: FLOAT AS gas_limit,
  t.value :gas_used :: FLOAT AS gas_used,
  case when t.value:success = 1 then true else false end as success

FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t