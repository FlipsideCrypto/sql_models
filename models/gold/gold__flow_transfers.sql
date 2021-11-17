{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['events', 'flow','gold']
) }}

SELECT
    'flow' AS blockchain,
    e.block_timestamp,
    e.block_number,
    e.tx_id,
    MAX(event_from) AS event_from,
    MAX(event_from_label_type) AS event_from_label_type,
    MAX(event_from_label_subtype) AS event_from_label_subtype,
    MAX(event_from_label) AS event_from_label,
    MAX(event_from_address_name) AS event_from_address_name,
    MAX(event_to) AS event_to,
    MAX(event_to_label_type) AS event_to_label_type,
    MAX(event_to_label_subtype) AS event_to_label_subtype,
    MAX(event_to_label) AS event_to_label,
    MAX(event_to_address_name) AS event_to_address_name,
    t.tx_type AS event_type,
    MAX(event_amount) AS event_amount,
    MAX(event_amount_usd) AS event_amount_usd,
    MAX(event_currency) AS event_currency
FROM
    {{ ref('gold__flow_events') }}
    e
    JOIN {{ ref('gold__flow_transactions') }}
    t
    ON e.tx_id = t.tx_id
WHERE
    t.tx_type = 'token_transfer'
    AND e.event_type IN (
        'tokens_deposited',
        'tokens_withdrawn'
    )
    AND event_currency = 'FLOW'

{% if is_incremental() %}
AND e.block_timestamp >= getdate() - INTERVAL '3 days'
{% endif %}
GROUP BY
    e.blockchain,
    e.block_timestamp,
    e.block_number,
    e.tx_id,
    t.tx_type
