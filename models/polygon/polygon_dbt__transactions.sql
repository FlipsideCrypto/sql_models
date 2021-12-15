{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'polygon_silver', 'polygon_dbt_transactions','polygon']
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
    record_content :model :name :: STRING = 'polygon_txs_model'

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
  A.value :fee :: FLOAT AS fee,
  A.value :from_address :: STRING AS from_address,
  A.value :gas_limit :: INT AS gas_limit,
  A.value :gas_price :: INT AS gas_price,
  A.value :gas_used :: INT AS gas_used,
  A.value :input_method :: STRING AS input_method,
  A.value :native_value :: FLOAT AS native_value,
  A.value :nonce :: INT AS nonce,
  A.value :success :: BOOLEAN AS success,
  A.value :to_address :: STRING AS to_address,
  A.value :tx_id :: STRING AS tx_id,
  A.value :tx_position :: INT AS tx_position
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) A
