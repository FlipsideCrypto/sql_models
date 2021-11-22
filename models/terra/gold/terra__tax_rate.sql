{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', block_number)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'tax_rate']
) }}

SELECT
  chain_id AS blockchain,
  block_timestamp,
  block_number,
  tax_rate
FROM
  {{ ref(
    'silver_terra__tax_rate'
  ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
