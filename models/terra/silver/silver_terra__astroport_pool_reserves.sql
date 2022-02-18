{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, contract_address)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'astroport', 'pool_reserves']
) }}

SELECT
  *
FROM
  {{ ref('terra_dbt__astroport_pool_reserves') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS msgs
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id, contract_address
ORDER BY
  system_created_at DESC)) = 1
