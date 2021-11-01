{{ config(
  materialized='incremental',
  cluster_by=['block_timestamp'],
  unique_key='block_timestamp',
  tags=['custom', 'snowflake','gold','validators','near'])
}}

SELECT
  'near' as blockchain,
  v.block_timestamp,
  v.block_id as block_number,
  v.account_id as address,
  address_labels.l1_label as address_label_type,
  address_labels.l2_label as address_label_subtype,
  address_labels.project_name as address_label,
  address_labels.address_name as address_name,
  v.stake / 10e24 as stake_amount,
  (v.stake / 10e24) * stake_price.price as stake_amount_usd,
  v.is_slashed,
  v.status,
  v.expected_blocks,
  v.produced_blocks,
  v.epoch_start_block
FROM
  {{ source('near', 'near_validators')}} v
LEFT OUTER JOIN
  {{ ref('near_address_labels')}} as address_labels
ON
  v.account_id = address_labels.address
LEFT OUTER JOIN
  {{ ref('near_prices')}} stake_price
ON
  stake_price.hour = date_trunc('hour', v.block_timestamp)
  AND stake_price.symbol = 'NEAR'
WHERE
  {% if is_incremental() %}
    block_timestamp >= getdate() - interval '3 days'
  {% else %}
    block_timestamp >= getdate() - interval '9 months'
  {% endif %}
