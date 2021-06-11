{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'terra', 'staking']
  )
}}

WITH delegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id, 
    'delegate' AS action,
    delegator_address,
    validator AS validator_address,
    event_transfer_amount AS amount,
    event_transfer_currency AS currency
  FROM {{ ref('terra__delegate') }}
),
undelegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id, 
    'undelegate' AS action,
    delegator_address,
    validator_address AS validator_address,
    CASE WHEN event_transfer_1_amount IS NOT NULL THEN event_transfer_1_amount ELSE event_amount END AS amount,
    CASE WHEN event_transfer_1_currency IS NOT NULL THEN event_transfer_1_currency ELSE event_currency END AS currency
  FROM {{ ref('terra__undelegate') }}
),
redelegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id, 
    'redelegate' AS action,
    delegator_address,
    validator_dst_address AS validator_address,
    event_amount AS amount,
    event_currency AS currency
    FROM {{ ref('terra__redelegate') }}
),
prices AS (
SELECT
    p.symbol,
    date_trunc('day', block_timestamp) as day,
    avg(price_usd) as price
FROM
    {{ ref('terra__oracle_prices')}} p
WHERE
    {% if is_incremental() %}
        block_timestamp >= getdate() - interval '3 days'
    {% else %}
        block_timestamp >= getdate() - interval '12 months'
    {% endif %}
GROUP BY p.symbol, day
)

SELECT 
    a.blockchain,
    a.chain_id,
    a.tx_status,
    a.block_id,
    a.block_timestamp,
    date_trunc('day', a.block_timestamp) as day,
    a.tx_id, 
    a.action,
    a.delegator_address,
    address_labels_delegator.l1_label as address_label_delegator_type,
    address_labels_delegator.l2_label as address_label_delegator_subtype,
    address_labels_delegator.project_name as address_delegator_label,
    address_labels_delegator.address_name as address_delegator_address_name,
    a.validator_address,
    address_labels_validator.l1_label as address_label_validator_type,
    address_labels_validator.l2_label as address_label_validator_subtype,
    address_labels_validator.project_name as address_validator_label,
    address_labels_validator.address_name as address_validator_address_name,
    a.amount,
    p.symbol AS currency
FROM (
    SELECT * FROM delegate
    UNION ALL 
    SELECT * FROM undelegate
    UNION ALL 
    SELECT * FROM redelegate
) a

LEFT OUTER JOIN
  prices p
ON
  p.symbol = a.currency
  AND p.day = date_trunc('day', a.block_timestamp)

LEFT OUTER JOIN
  {{source('shared','udm_address_labels')}} address_labels_delegator
ON
  a.address = address_labels_delegator.address

LEFT OUTER JOIN
  {{source('shared','udm_address_labels')}} address_labels_validator
ON
  a.address = address_labels_validator.address