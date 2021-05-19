{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'events']
  )
}}

WITH prices AS (
  SELECT
    p.symbol,
    date_trunc('hour', recorded_at) as hour,
    avg(price) as price
  FROM {{ source('shared', 'prices')}} p
  WHERE
    p.asset_id = 4030
    {% if is_incremental() %}
      AND recorded_at >= getdate() - interval '7 days'
    {% else %}
      AND recorded_at >= getdate() - interval '9 months'
    {% endif %}
  GROUP BY p.symbol, hour
),

tx_asset_type AS (
  SELECT
    -- blockchain,
    -- block_timestamp,
    -- block_number,
    tx_id AS tx_id_asset,
    CASE WHEN event_currency = 'ALGO' THEN 'ALGO' ELSE 'ASAs' END AS asset_type,
    decimal_adjustment_asset,
    price_asset
  FROM (
    SELECT DISTINCT     
      'algorand' as blockchain,
      block_timestamp,
      block_id as block_number,
      tx_id, 
      coalesce(adj.symbol, event_currency) as event_currency,
      adj.decimal_adjustment as decimal_adjustment_asset,
      prices.price as price_asset
    FROM {{ source('algorand', 'udm_events_algorand')}} e

    LEFT OUTER JOIN {{ source('shared', 'udm_decimal_adjustments')}} adj
      ON e.event_currency = adj.token_identifier AND adj.blockchain = 'algorand'

    LEFT OUTER JOIN prices
      ON prices.hour = date_trunc('hour', e.block_timestamp) 
        AND prices.symbol = e.event_currency

    WHERE e.event_currency IS NOT NULL 
        AND 
        {% if is_incremental() %}
          block_timestamp >= getdate() - interval '7 days'
        {% else %}
          block_timestamp >= getdate() - interval '9 months'
        {% endif %}
        AND decimal_adjustment_asset IS NOT NULL
  )
)

SELECT
  'algorand' as blockchain,
  block_timestamp,
  block_id as block_number,
  tx_id,
  tx_from,
  tx_from_labels.l1_label as tx_from_label_type,
  tx_from_labels.l2_label as tx_from_label_subtype,
  tx_from_labels.project_name as tx_from_label,
  tx_from_labels.address_name as tx_from_address_name,
  tx_to,
  tx_to_labels.l1_label as tx_to_label_type,
  tx_to_labels.l2_label as tx_to_label_subtype,
  tx_to_labels.project_name as tx_to_label,
  tx_to_labels.address_name as tx_to_address_name,
  tx_type,
  CASE WHEN tx_asset_type.asset_type = 'ALGO' THEN tx_fee / pow(10, COALESCE(tx_asset_type.decimal_adjustment_asset, 0)) 
    ELSE tx_fee / pow(10, COALESCE(adj.decimal_adjustment, 0))
    END AS tx_fee,
  -- tx_fee / pow(10, COALESCE(adj.decimal_adjustment, 0)) AS tx_fee,
  -- CASE WHEN tx_id IN (SELECT DISTINCT tx_id FROM {{ source('algorand', 'udm_events_algorand')}} WHERE event_currency = 'ALGO') THEN tx_fee / pow(10, COALESCE(adj.decimal_adjustment, 0))
  -- ELSE tx_fee END AS tx_fee,
  -- CASE WHEN tx_id IN (SELECT DISTINCT tx_id FROM {{ source('algorand', 'udm_events_algorand')}} WHERE event_currency = 'ALGO') THEN tx_fee / pow(10, COALESCE(adj.decimal_adjustment, 0)) * prices.price
  -- ELSE NULL END AS tx_fee_usd,
  CASE WHEN tx_asset_type.asset_type = 'ALGO' THEN tx_fee / pow(10, COALESCE(tx_asset_type.decimal_adjustment_asset, 0)) * tx_asset_type.price_asset
    ELSE tx_fee / pow(10, COALESCE(adj.decimal_adjustment, 0)) * prices.price
    END AS tx_fee_usd,
  -- tx_fee / pow(10, COALESCE(adj.decimal_adjustment, 0)) * prices.price AS tx_fee_usd,
  event_from,
  event_from_labels.l1_label as event_from_label_type,
  event_from_labels.l2_label as event_from_label_subtype,
  event_from_labels.project_name as event_from_label,
  event_from_labels.address_name as event_from_address_name,
  event_to,
  event_to_labels.l1_label as event_to_label_type,
  event_to_labels.l2_label as event_to_label_subtype,
  event_to_labels.project_name as event_to_label,
  event_to_labels.address_name as event_to_address_name,
  event_type,
  -- CASE WHEN tx_id IN (SELECT DISTINCT tx_id FROM {{ source('algorand', 'udm_events_algorand')}} WHERE event_currency = 'ALGO') THEN event_amount / pow(10, COALESCE(adj.decimal_adjustment, 0))
  -- ELSE event_amount END AS event_amount,
  event_amount / pow(10, COALESCE(adj.decimal_adjustment, 0)) AS event_amount,
  -- CASE WHEN tx_id IN (SELECT DISTINCT tx_id FROM {{ source('algorand', 'udm_events_algorand')}} WHERE event_currency = 'ALGO') THEN event_amount / pow(10, COALESCE(adj.decimal_adjustment, 0)) * prices.price
  -- ELSE NULL END AS event_amount_usd,
  event_amount / pow(10, COALESCE(adj.decimal_adjustment, 0)) * prices.price AS event_amount_usd,
  coalesce(adj.symbol, event_currency) as event_currency,
  tx_asset_type.decimal_adjustment_asset,
  tx_asset_type.price_asset
FROM {{ source('algorand', 'udm_events_algorand')}} e

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels_new')}} as tx_from_labels
    ON e.tx_from = tx_from_labels.address AND tx_from_labels.blockchain = 'algorand'

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels_new')}} as tx_to_labels
    ON e.tx_to = tx_to_labels.address AND tx_to_labels.blockchain = 'algorand'

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels_new')}} as event_from_labels
    ON e.event_from = event_from_labels.address AND event_from_labels.blockchain = 'algorand'

LEFT OUTER JOIN
  {{ source('shared', 'udm_address_labels_new')}} as event_to_labels
    ON e.event_to = event_to_labels.address AND event_to_labels.blockchain = 'algorand'

LEFT OUTER JOIN
  {{ source('shared', 'udm_decimal_adjustments')}} adj
    ON e.event_currency = adj.token_identifier AND adj.blockchain = 'algorand'
    -- ON e.blockchain = adj.blockchain AND adj.blockchain = 'algorand'

LEFT OUTER JOIN prices
  ON prices.hour = date_trunc('hour', e.block_timestamp) 
  AND prices.symbol = e.event_currency

LEFT OUTER JOIN tx_asset_type
  ON e.tx_id = tx_asset_type.tx_id_asset

WHERE 
  {% if is_incremental() %}
    block_timestamp >= getdate() - interval '7 days'
  {% else %}
    block_timestamp >= getdate() - interval '9 months'
  {% endif %}