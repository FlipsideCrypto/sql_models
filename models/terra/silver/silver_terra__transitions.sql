{{ config(
  materialized = 'incremental',
  unique_key = 'chain_id || block_id || index || transition_type || event',
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp', 'block_id'],
  tags = ['snowflake', 'terra_silver', 'terra_transitions']
) }}

SELECT
  *
FROM
  {{ ref('terra_dbt__transitions') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND system_created_at :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }} AS transitions
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, INDEX, transition_type, event
ORDER BY
  system_created_at DESC)) = 1
