{{ config(
  materialized='incremental',
  unique_key='date',
  incremental_strategy='delete+insert',
  cluster_by=['date'],
  tags=['snowflake', 'gold', 'near', 'gold__near_reward_flow'],
)}}
WITH near_labels AS (
    SELECT 
        l1_label,
        l2_label,
        project_name,
        address_name,
        address
    FROM 
    {{ source(
        'shared',
        'udm_address_labels'
    )}}
    WHERE blockchain = 'near'
), near_prices AS (
    SELECT
      symbol,
      date_trunc('day', recorded_at) as day,
      avg(price) as price
    FROM
    {{ source(
        'shared',
        'prices'
    )}}
    WHERE symbol = 'NEAR'
    GROUP BY symbol, day
)
SELECT
  'near' as blockchain,
  xfer_date as date,
  entity_id as address,
  address_labels.l1_label as address_label_type,
  address_labels.l2_label as address_label_subtype,
  address_labels.project_name as address_label,
  address_labels.address_name as address_name,
  metric_slug,
  metric_value,
  metric_value * p.price as metric_value_usd
FROM
  {{ source('near', 'near_daily_reward_flow')}} f
LEFT OUTER JOIN
  near_labels as address_labels
ON
  f.entity_id = address_labels.address
LEFT OUTER JOIN
  near_prices p
ON
  p.day = xfer_date
  AND p.symbol = 'NEAR'
WHERE
  {% if is_incremental() %}
    xfer_date >= getdate() - interval '3 days'
  {% else %}
    xfer_date >= getdate() - interval '9 months'
  {% endif %}
