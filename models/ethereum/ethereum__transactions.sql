{{ 
  config(
    materialized='incremental',
    unique_key='block_id || tx_id', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp'],
    tags=['snowflake', 'ethereum', 'events', 'transactions', 'ethereum_transactions', 'address_labels']
  )
}}

WITH events AS (
  SELECT
  tx_hash,
  event_count,
  tx_fee
  FROM (
    SELECT *,
    count(case when log_index IS NOT NULL then 1 end) over(partition by tx_hash) as event_count,
    max(fee) over(partition by tx_hash) as tx_fee
    FROM
      {{ref('silver_ethereum__events')}}
    WHERE 1=1
      {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
      {% endif %}
    )
  GROUP BY 1,2,3
),
txn AS (
  SELECT
  *
  FROM
  {{ref('silver_ethereum__transactions')}}
  WHERE 1=1
    {% if is_incremental() %}
      AND block_timestamp >= getdate() - interval '2 days'
    {% endif %}
),
eth_prices AS (
    SELECT
      p.symbol,
      date_trunc('hour', recorded_at) as hour,
      avg(price) as price
    FROM
      {{source('shared', 'prices')}} p
    JOIN
      {{source('shared', 'cmc_assets')}} a
    ON
      p.asset_id = a.asset_id
    WHERE
        a.asset_id = 1027
        {% if is_incremental() %}
          AND recorded_at >= getdate() - interval '2 days'
        {% endif %}
    GROUP BY p.symbol, hour
)
SELECT
   t.block_timestamp,
   t.block_id as block_id,
   t.tx_hash as tx_id,
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
   tx_fee,
   tx_fee * p.price as fee_usd,
   case when success = 1 then true else false end as success,
   event_count
  FROM
     events e
  JOIN
    txn t
  ON
    e.tx_hash = t.tx_hash
  LEFT OUTER JOIN
    {{source('ethereum', 'sha256_function_signatures')}} f
  ON
    t.input_method = f.hex_signature
  AND f.importance = 1
  LEFT OUTER JOIN
    {{ source('ethereum', 'ethereum_token_contracts') }} c
  ON
   t.to_address = c.contract_address
  LEFT OUTER JOIN
     eth_prices p
  ON
    date_trunc('hour', t.block_timestamp) = p.hour
  LEFT OUTER JOIN
    {{ ref('silver_crosschain__address_labels') }} as from_labels
  ON
    from_address = from_labels.address AND from_labels.blockchain = 'ethereum' AND from_labels.creator = 'flipside'
  LEFT OUTER JOIN
    {{ ref('silver_crosschain__address_labels') }} as to_labels
  ON
    to_address = to_labels.address AND to_labels.blockchain = 'ethereum' AND to_labels.creator = 'flipside'
