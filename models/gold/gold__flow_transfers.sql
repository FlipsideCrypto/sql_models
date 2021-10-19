{{ 
    config(
        materialized='incremental', 
        sort='block_timestamp', 
        dist='tx_id', 
        unique_key='tx_id', 
        tags=['events', 'flow']
    ) 
}}

SELECT
    'flow' as blockchain,
    e.block_timestamp,
    e.block_number,
    e.tx_id,
    max(event_from) as event_from,
    max(event_from_label_type) as event_from_label_type,
    max(event_from_label_subtype) as event_from_label_subtype,
    max(event_from_label) as event_from_label,
    max(event_from_address_name) as event_from_address_name,
    max(event_to) as event_to,
    max(event_to_label_type) as event_to_label_type,
    max(event_to_label_subtype) as event_to_label_subtype,
    max(event_to_label) as event_to_label,
    max(event_to_address_name) as event_to_address_name,
    t.tx_type as event_type,
    max(event_amount) as event_amount,
    max(event_amount_usd) as event_amount_usd,
    max(event_currency) as event_currency
FROM
    {{ ref('flow_events') }} e
JOIN
    {{ ref('flow_transactions') }} t
ON
    e.tx_id = t.tx_id
WHERE
    t.tx_type = 'token_transfer'
AND e.event_type IN ('tokens_deposited', 'tokens_withdrawn') AND event_currency = 'FLOW'
{% if is_incremental() %}
  AND e.block_timestamp >= getdate() - interval '1 days'
{% else %}
  AND e.block_timestamp >= getdate() - interval '9 months'
{% endif %}
GROUP BY
e.blockchain,
e.block_timestamp,
e.block_number,
e.tx_id,
t.tx_type