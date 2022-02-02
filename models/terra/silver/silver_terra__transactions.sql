{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_transactions']
) }}

SELECT
  *
FROM
  {{ ref('terra_dbt__transactions') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, tx_id
ORDER BY
  system_created_at DESC)) = 1
