{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'set_ip_address_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.ip_addr,
  e.node_addr AS node_address
FROM
  {{ ref('silver_thorchain__set_ip_address_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
{% if is_incremental() %}
WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}