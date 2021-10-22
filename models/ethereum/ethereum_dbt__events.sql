{{ config(
  materialized = 'incremental',
  unique_key = 'block_id || tx_hash || log_index || to',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'silver_ethereum', 'ethereum_dbt__events']
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
      'udm_events'
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
  t.value :input_method :: STRING AS input_method,
  t.value :from :: STRING AS "from",
  t.value :to :: STRING AS "to",
  t.value :name :: STRING AS name,
  t.value :symbol :: STRING AS symbol,
  t.value :contract_address :: STRING AS contract_address,
  t.value :eth_value :: FLOAT AS eth_value,
  t.value :fee :: FLOAT AS fee,
  t.value :log_index :: INTEGER AS log_index,
  t.value :log_method :: STRING AS log_method,
  t.value :token_value :: FLOAT as token_value
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t