{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra_silver', 'terra_transactions']
) }}

SELECT
  system_created_at,
  _inserted_timestamp,
  blockchain,
  block_id,
  block_timestamp,
  chain_id,
  codespace,
  tx_id,
  tx_from,
  tx_to,
  tx_type,
  tx_module,
  tx_status,
  tx_status_msg,
  tx_code,
  fee,
  gas_wanted,
  gas_used
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
