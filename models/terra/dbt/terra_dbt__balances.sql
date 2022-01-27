{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', address, block_number)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'terra_silver', 'terra_balances']
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
      'terra_balances',
      'terra-5_balances',
      'terra-5_synthetic_balances_model',
      'terra_synthetic_balances_model'
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
  t.value :address :: STRING AS address,
  t.value :balance :: INT AS balance,
  t.value :balance_type :: STRING AS balance_type,
  t.value :block_id :: INT AS block_number,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :blockchain :: STRING AS blockchain,
  t.value :currency :: STRING AS currency
FROM
  base_tables,
  LATERAL FLATTEN(
    input => record_content :results
  ) t
