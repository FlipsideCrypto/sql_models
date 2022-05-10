{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'thorname_change_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.owner,
  e.chain,
  e.address,
  e.expire,
  e.name,
  e.fund_amount_e8,
  e.registration_fee_e8
FROM
  {{ ref(
    'silver_thorchain__thorname_change_events'
  ) }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
