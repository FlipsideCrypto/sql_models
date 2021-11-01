{{ config(
  materialized = 'incremental',
  unique_key = 'chain_id || block_id || tx_id || msg_index',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id', 'tx_id'],
  tags = ['snowflake', 'terra_silver', 'terra_msgs']
) }}

SELECT
  *
FROM
  {{ ref('terra_dbt__msgs') }}
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

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id, msg_index
ORDER BY
  system_created_at DESC)) = 1
