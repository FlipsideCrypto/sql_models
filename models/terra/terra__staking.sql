{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
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
    validator_address,
    event_amount AS amount,
    event_currency AS currency
  FROM {{ ref('terra_dbt__delegate') }}
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
    validator_address,
    event_amount AS amount,
    event_currency AS currency
  FROM {{ ref('terra_dbt__undelegate') }}
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
    FROM {{ ref('terra_dbt__redelegate') }}
),
prices AS (
    SELECT 
      date_trunc('hour', block_timestamp) as hour,
      currency,
      symbol,
      avg(price_usd) as price_usd
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
    a.delegator_address,
    delegator_labels.l1_label as delegator_label_type,
    delegator_labels.l2_label as delegator_label_subtype,
    delegator_labels.project_name as delegator_address_label,
    delegator_labels.address_name as delegator_address_name,
    a.validator_address,
    validator_labels.l1_label as validator_label_type,
    validator_labels.l2_label as validator_label_subtype,
    validator_labels.project_name as validator_address_label,
    validator_labels.address_name as validator_address_name,
    a.amount as event_amount,
    price_usd,
    a.amount * price_usd as event_amount_usd,
    p.symbol AS currency
FROM (
    SELECT * FROM delegate
    UNION ALL 
    SELECT * FROM undelegate
    UNION ALL 
    SELECT * FROM redelegate
) a

LEFT OUTER JOIN prices p
  ON p.currency = a.currency
  AND p.hour = date_trunc('hour', a.block_timestamp)

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} delegator_labels
  ON a.delegator_address = delegator_labels.address

LEFT OUTER JOIN {{source('shared','udm_address_labels_new')}} validator_labels
  ON a.validator_address = validator_labels.address