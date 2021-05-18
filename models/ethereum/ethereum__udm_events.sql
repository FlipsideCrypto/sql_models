{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'ethereum', 'events']
  )
}}

WITH token_prices AS (
  SELECT
    p.symbol,
    date_trunc('hour', recorded_at) as hour,
    lower(a.token_address) as token_address,
    avg(price) as price
  FROM
    {{ source('shared', 'prices')}} p

  JOIN
    {{ source('shared', 'cmc_assets')}} a
      ON p.asset_id = a.asset_id

  WHERE
      a.platform_id = 1027

     {% if is_incremental() %}
       AND recorded_at >= getdate() - interval '2 days'
     {% else %}
       AND recorded_at >= getdate() - interval '9 months'
     {% endif %}
      
  GROUP BY p.symbol, hour, token_address
), events AS (
  SELECT
    block_timestamp,
    block_id,
    tx_hash as tx_id,
    "from" as from_address,
    from_labels.l1_label as from_label_type,
    from_labels.l2_label as from_label_subtype,
    from_labels.project_name as from_label,
    from_labels.address_name as from_address_name,
    "to" as to_address,
    to_labels.l1_label as to_label_type,
    to_labels.l2_label as to_label_subtype,
    to_labels.project_name as to_label,
    to_labels.address_name as to_address_name,
    log_method as event_name,
    null as event_type,
    log_index as event_id,
    contract_address,
    coalesce(e.symbol, contract_labels.address_name) as symbol,
    input_method,
    eth_value,
    token_value,
    fee
  FROM {{source('ethereum','ethereum_events')}} e

  LEFT OUTER JOIN
    {{ source('ethereum', 'ethereum_address_labels') }} as from_labels
      ON e."from" = from_labels.address

  LEFT OUTER JOIN
    {{ source('ethereum', 'ethereum_address_labels') }} as to_labels
      ON e."to" = to_labels.address

  LEFT OUTER JOIN
    {{ source('ethereum', 'ethereum_address_labels') }} as contract_labels
    ON e.contract_address = contract_labels.address

  WHERE
    {% if is_incremental() %}
      block_timestamp >= getdate() - interval '2 days'
    {% else %}
      block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),
originator AS (
  SELECT DISTINCT
    tx_id,
    from_address as origin_address,
    from_label_type as origin_label_type,
    from_label_subtype as origin_label_subtype,
    from_label as origin_label,
    from_address_name as origin_address_name,
    input_method as origin_function_signature,
    f.text_signature as origin_function_name
  FROM events e
  LEFT OUTER JOIN
    {{ source('ethereum', 'sha256_function_signatures') }} as f
      ON e.input_method = f.hex_signature AND f.importance = 1
  WHERE (e.event_id IS NULL AND e.contract_address IS NULL AND e.eth_value = 0) OR e.fee > 0
),
full_events AS (
  SELECT
    block_timestamp,
    block_id,
    e.tx_id,
    origin_address,
    origin_label_type,
    origin_label_subtype,
    origin_label,
    origin_address_name,
    origin_function_signature,
    origin_function_name,
    from_address,
    from_label_type,
    from_label_subtype,
    from_label,
    from_address_name,
    to_address,
    to_label_type,
    to_label_subtype,
    to_label,
    to_address_name,
    event_name,
    event_type,
    event_id,
    contract_address,
    symbol,
    eth_value,
    token_value
  FROM events e
  JOIN originator o 
    ON e.tx_id = o.tx_id
),
token_transfers AS (
  SELECT
    block_timestamp,
    block_id,
    tx_id,
    origin_address,
    origin_label_type,
    origin_label_subtype,
    origin_label,
    origin_address_name,
    origin_function_signature,
    origin_function_name,
    from_address,
    from_label_type,
    from_label_subtype,
    from_label,
    from_address_name,
    to_address,
    to_label_type,
    to_label_subtype,
    to_label,
    to_address_name,
    'transfer' as event_name,
    'erc20_transfer' as event_type,
    event_id,
    e.contract_address,
    coalesce(e.symbol, p.symbol) as symbol,
    token_value as amount,
    token_value * p.price  as amount_usd
  FROM
     full_events e
  LEFT OUTER JOIN
    token_prices p
      ON p.token_address = e.contract_address
        AND date_trunc('hour', e.block_timestamp) = p.hour
  WHERE
    event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),
eth_prices AS (
  SELECT
    p.symbol,
    date_trunc('hour', recorded_at) as hour,
    avg(price) as price
  FROM {{ source('shared', 'prices')}} p
  JOIN
    {{ source('shared', 'cmc_assets')}} a
    ON p.asset_id = a.asset_id
  WHERE
    a.asset_id = 1027
    {% if is_incremental() %}
      AND recorded_at >= getdate() - interval '2 days'
    {% else %}
      AND recorded_at >= getdate() - interval '9 months'
    {% endif %}
  GROUP BY p.symbol, hour
),
eth_transfers AS (
  SELECT
    block_timestamp,
    block_id,
    tx_id,
    origin_address,
    origin_label_type,
    origin_label_subtype,
    origin_label,
    origin_address_name,
    origin_function_signature,
    origin_function_name,
    from_address,
    from_label_type,
    from_label_subtype,
    from_label,
    from_address_name,
    to_address,
    to_label_type,
    to_label_subtype,
    to_label,
    to_address_name,
    'transfer' as event_name,
    'native_eth' as event_type,
    event_id,
    NULL::text as contract_address,
    'ETH' as symbol,
    eth_value as amount,
    eth_value * p.price as amount_usd
  FROM
    full_events e
  LEFT OUTER JOIN 
    eth_prices p 
      ON date_trunc('hour', e.block_timestamp) = p.hour
  WHERE
    eth_value > 0
),
logs AS (
  SELECT
    block_timestamp,
    block_id,
    tx_id,
    origin_address,
    origin_label_type,
    origin_label_subtype,
    origin_label,
    origin_address_name,
    origin_function_signature,
    origin_function_name,
    NULL::text as from_address,
    NULL::text as from_label_type,
    NULL::text as from_label_subtype,
    NULL::text as from_label,
    NULL::text as from_address_name,
    NULL::text as to_address,
    NULL::text as to_label_type,
    NULL::text as to_label_subtype,
    NULL::text as to_label,
    NULL::text as to_address_name,
    coalesce(decoded_logs.method, event_name) as event_name,
    decoded_logs.type as event_type,
    event_id,
    contract_address,
    e.symbol,
    NULL::float as amount,
    NULL::float as amount_usd
  FROM full_events e
  LEFT OUTER JOIN 
    {{ source('ethereum', 'ethereum_decoded_log_methods')}} as decoded_logs
      ON e.event_name = decoded_logs.encoded_log_method
  WHERE
    event_name IS NOT NULL 
    AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
)
SELECT * FROM token_transfers
UNION ALL
SELECT * FROM eth_transfers
UNION ALL
SELECT * FROM logs
ORDER BY block_timestamp
