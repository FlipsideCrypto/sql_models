{{ config(
  materialized='incremental',
  unique_key='block_timestamp',
  incremental_strategy='delete+insert',
  cluster_by=['block_timestamp'],
  tags=['snowflake', 'gold', 'near', 'gold__near_validators']
)}}
WITH near_labels AS (
    SELECT 
        l1_label,
        l2_label,
        project_name,
        address_name,
        address
    FROM {{ source('shared', 'udm_address_labels') }}
    WHERE blockchain = 'near'
), near_prices AS (
    SELECT
        date_trunc('hour', recorded_at) as hour,
        price
    FROM {{ source('shared', 'prices') }}
    WHERE symbol = 'NEAR'
)
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
FROM {{ source('near', 'near_validators') }} v
LEFT OUTER JOIN near_labels as address_labels ON v.account_id = address_labels.address
LEFT OUTER JOIN near_prices as stake_price ON stake_price.hour = date_trunc('hour', v.block_timestamp)
WHERE
  {% if is_incremental() %}
    block_timestamp >= getdate() - interval '3 days'
  {% else %}
    block_timestamp >= getdate() - interval '9 months'
  {% endif %}
