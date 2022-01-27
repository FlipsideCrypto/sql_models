{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', block_number)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'tax_rate']
) }}

SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  _inserted_timestamp,
  record_content :model :executor :chain_id :: VARCHAR(10) AS chain_id,
  t.value :block_number AS block_number,
  t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
  t.value :tax_rate AS tax_rate
FROM
  {{ source(
    "bronze",
    "prod_terra_sink_645110886"
  ) }},
  LATERAL FLATTEN(
    input => record_content :results
  ) t
WHERE
  record_content :model :name :: STRING IN ('terra-5_tax_rate') qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_number
ORDER BY
  system_created_at DESC)) = 1

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
