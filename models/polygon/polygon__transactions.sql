{{ config(
    materialized='incremental', 
    unique_key='chain_id || block_id || tx_id', 
    incremental_strategy='delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags=['snowflake', 'polygon', 'polygon_transactions_gold']
)}}

WITH events AS (
    SELECT
        tx_id
        , sum(case when log_index is not null then 1 else 0 end) as event_count
    FROM
        {{ source('silver_polygon','udm_events') }}
    where 1=1
    {% if is_incremental() %}
    block_timestamp::date >= (select max(block_timestamp::date) from from {{source('polygon', 'transactions')}})
    {% endif %}
    group by 1
),
txn AS (
  SELECT
  *
  FROM
  {{source('silver_polygon', 'transactions')}}
  WHERE 1=1
    {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from from {{source('polygon', 'transactions')}})
    {% endif %}
),
poly_labels AS (
    SELECT
        l1_label,
        l2_label,
        project_name,
        address_name,
        address
    FROM
        {{ source('shared','udm_address_labels_new') }}
    WHERE
        blockchain = 'polygon'
),
poly_prices AS (
  SELECT
    p.symbol,
    date_trunc('hour', recorded_at) as hour,
    avg(price) as price
  FROM {{ source('shared','prices_v2') }} p
  WHERE 
    p.asset_id = '3890'
  {% if is_incremental() %}
  AND  p.recorded_at::date >= (select max(block_timestamp::date) from from {{source('polygon', 'transactions')}})
  {% endif %}
  group by 1,2
  )
SELECT
   t.block_timestamp,
   t.block_id as block_id,
   t.tx_id,
   tx_position,
   nonce,
   from_address,
   from_labels.l1_label as from_label_type,
   from_labels.l2_label as from_label_subtype,
   from_labels.project_name as from_label,
   from_labels.address_name as from_address_name,
   to_address,
   to_labels.l1_label as to_label_type,
   to_labels.l2_label as to_label_subtype,
   to_labels.project_name as to_label,
   to_labels.address_name as to_address_name,
   c.symbol,
   t.input_method as function_signature,
   f.text_signature as function_name,
   gas_price,
   gas_limit,
   gas_used,
   fee,
   fee * p.price as fee_usd,
   case when success = 1 then true else false end as success,
   event_count
  FROM txn t
  LEFT JOIN events e ON e.tx_id = t.tx_id
  LEFT JOIN {{source('ethereum', 'sha256_function_signatures')}} f ON t.input_method = f.hex_signature
                                                                        AND f.importance = 1
  LEFT JOIN {{ source('shared','market_asset_metadata') }}  c ON lower(t.to_address) = lower(c.token_address)
  LEFT JOIN poly_prices p ON date_trunc('hour', t.block_timestamp) = p.hour
  LEFT JOIN poly_labels as from_labels ON from_address = from_labels.address
  LEFT JOIN poly_labels as to_labels ON to_address = to_labels.address
