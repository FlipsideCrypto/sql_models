{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'update_node_account_status_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.node_addr AS node_address,
  e."CURRENT_FLAG" AS current_status,
  e.former AS former_status
FROM
  {{ ref('silver_thorchain__update_node_account_status_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
        {% if is_incremental() %}
WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}