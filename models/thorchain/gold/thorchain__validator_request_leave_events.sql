{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'validator_request_leave_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.from_addr AS from_address,
  e.node_addr AS node_address
FROM
  {{ ref('silver_thorchain__validator_request_leave_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
        {% if is_incremental() %}
WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}