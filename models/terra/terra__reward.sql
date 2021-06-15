{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'terra', 'reward']
  )
}}

WITH withdraw_delegator_rewards AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id, 
    'withdraw_delegator_rewards' AS action,
    event_transfer_amount AS amount,
    event_transfer_currency AS currency,
    recipient,
    validator
  FROM {{ ref('terra_dbt__withdraw_delegator_rewards') }}
),

withdraw_validator_commission AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id, 
    'withdraw_validator_commission' AS action,
    amount,
    currency,
    recipient,
    validator_address AS validator
  FROM {{ ref('terra_dbt__withdraw_validator_commission') }}
),
prices AS (
    SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(luna_exchange_rate) as luna_exchange_rate,
      avg(price_usd) as price_usd,
      avg(luna_usd_price) as luna_usd_price
    FROM {{ ref('terra__oracle_prices')}} 
    WHERE
    {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
    {% else %}
      block_timestamp >= getdate() - interval '9 months'
    {% endif %} 
    GROUP BY 1,2,3
)

SELECT 
    a.blockchain,
    a.chain_id,
    a.tx_status,
    a.block_id,
    a.block_timestamp,
    a.tx_id, 
    a.action,
    a.recipient,
    recipient_labels.l1_label as recipient_label_type,
    recipient_labels.l2_label as recipient_label_subtype,
    recipient_labels.project_name as recipient_address_label,
    recipient_labels.address_name as recipient_address_name,
    a.validator AS validator,
    validator_labels.l1_label as validator_label_type,
    validator_labels.l2_label as validator_label_subtype,
    validator_labels.project_name as validator_address_label,
    validator_labels.address_name as validator_address_name,
    a.amount as event_amount,
    price_usd,
    a.amount * price_usd as event_amount_usd,
    p.symbol AS currency
FROM (
    SELECT * FROM withdraw_delegator_rewards
    UNION ALL 
    SELECT * FROM withdraw_validator_commission
) a
LEFT OUTER JOIN prices p
  ON p.currency = a.currency
  AND p.hour = date_trunc('hour', a.block_timestamp)

LEFT OUTER JOIN {{source('shared','udm_address_labels')}} recipient_labels
  ON a.recipient = recipient_labels.address

LEFT OUTER JOIN {{source('shared','udm_address_labels')}} validator_labels
  ON a.validator = validator_labels.address
