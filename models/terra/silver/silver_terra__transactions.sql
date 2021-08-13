{{ config(
  materialized = 'incremental',
  unique_key = 'chain_id || block_id || tx_id',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'terra_silver', 'terra_transactions']
) }}

SELECT
  *
FROM
  {{ ref('terra_dbt__transactions') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ source(
      'silver_terra',
      'transactions'
    ) }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id
ORDER BY
  system_created_at DESC)) = 1
