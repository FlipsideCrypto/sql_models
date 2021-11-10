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
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
UNION ALL
  /*
            Columbus-4 tax data
            # TODO: backfill bronze and remove this query
     */
SELECT
  NULL :: TIMESTAMP AS system_created_at,
  'colubus-4' :: VARCHAR(10) AS chain_id,
  block_number,
  block_timestamp,
  tax_rate
FROM
  {{ source(
    'terra',
    'udm_custom_fields_terra_tax_rate'
  ) }}
WHERE
  block_number < 4724001 -- Columubs-5 Genesis block
  qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_number
ORDER BY
  system_created_at DESC)) = 1
