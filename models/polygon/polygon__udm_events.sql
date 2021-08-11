{{ config(
    materialized = 'incremental',
    unique_key = 'chain_id || block_id || tx_id || log_index',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags = ['snowflake', 'polygon', 'events']
) }}

WITH token_prices AS (

    SELECT
        p.symbol,
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS HOUR,
        LOWER(
            A.token_address
        ) AS token_address,
        AVG(price) AS price
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
        p
        JOIN {{ source(
            'shared',
            'market_asset_metadata'
        ) }} A
        ON p.asset_id :: STRING = A.asset_id :: STRING
    WHERE
        A.platform = 'polygon'

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '2 days'
{% else %}
    AND recorded_at >= getdate() - INTERVAL '9 months'
{% endif %}

-- TODO fix this shit
GROUP BY
    p.symbol,
    HOUR,
    token_address
),
poly_labels AS (
    SELECT
        l1_label,
        l2_label,
        project_name,
        address_name,
        address
    FROM
        {{ source(
            'shared',
            'udm_address_labels_new'
        ) }}
    WHERE
        blockchain = 'polygon'
),
base_udm_events AS (
    SELECT
        *
    FROM
        {{ source(
            'silver_polygon',
            'udm_events'
        ) }}
    WHERE

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
    block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
),
base_tx AS (
    SELECT
        *
    FROM
        {{ source(
            'silver_polygon',
            'transactions'
        ) }}
    WHERE

{% if is_incremental() %}
block_timestamp >= getdate() - INTERVAL '2 days'
{% else %}
    block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
),
events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        from_address,
        from_labels.l1_label AS from_label_type,
        from_labels.l2_label AS from_label_subtype,
        from_labels.project_name AS from_label,
        from_labels.address_name AS from_address_name,
        to_address,
        to_labels.l1_label AS to_label_type,
        to_labels.l2_label AS to_label_subtype,
        to_labels.project_name AS to_label,
        to_labels.address_name AS to_address_name,
        log_method AS event_name,
        NULL AS event_type,
        log_index AS event_id,
        contract_address,
        CASE
            WHEN e.symbol = 'ETH' THEN 'MATIC'
            ELSE e.symbol
        END AS symbol,
        input_method,
        native_value,
        token_value,
        fee
    FROM
        base_udm_events e
        LEFT OUTER JOIN poly_labels AS from_labels
        ON e.from_address = from_labels.address
        LEFT OUTER JOIN poly_labels AS to_labels
        ON e.to_address = to_labels.address
        LEFT OUTER JOIN poly_labels AS contract_labels
        ON e.contract_address = contract_labels.address
),
originator AS (
    SELECT
        tx_id,
        t.from_address AS origin_address,
        from_labels.l1_label AS origin_label_type,
        from_labels.l2_label AS origin_label_subtype,
        from_labels.project_name AS origin_label,
        from_labels.address_name AS origin_address_name,
        t.input_method AS origin_function_signature,
        f.text_signature AS origin_function_name
    FROM
        base_tx t
        LEFT OUTER JOIN {{ source(
            'ethereum',
            'sha256_function_signatures'
        ) }} AS f
        ON t.input_method = f.hex_signature
        AND f.importance = 1
        LEFT OUTER JOIN poly_labels AS from_labels
        ON t.from_address = from_labels.address
),
full_events AS (
    SELECT
        block_timestamp,
        block_id,
        e.tx_id,
        o.origin_address,
        o.origin_label_type,
        o.origin_label_subtype,
        o.origin_label,
        o.origin_address_name,
        o.origin_function_signature,
        o.origin_function_name,
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
    FROM
        events e
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
        'transfer' AS event_name,
        'erc20_transfer' AS event_type,
        event_id,
        e.contract_address,
        COALESCE(
            e.symbol,
            p.symbol
        ) AS symbol,
        token_value AS amount,
        token_value * p.price AS amount_usd
    FROM
        full_events e
        LEFT OUTER JOIN token_prices p
        ON p.token_address = e.contract_address
        AND DATE_TRUNC(
            'hour',
            e.block_timestamp
        ) = p.hour
    WHERE
        event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),
poly_prices AS (
  SELECT
    p.symbol,
    date_trunc('hour', recorded_at) as hour,
    avg(price) as price
  FROM {{ source('shared', 'prices_v2')}} p
  WHERE
    p.asset_id = '3890'
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
        'transfer' AS event_name,
        'native_eth' AS event_type,
        event_id,
        NULL :: text AS contract_address,
        'ETH' AS symbol,
        eth_value AS amount,
        eth_value * p.price AS amount_usd
    FROM
        full_events e
        LEFT OUTER JOIN eth_prices p
        ON DATE_TRUNC(
            'hour',
            e.block_timestamp
        ) = p.hour
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
        NULL :: text AS from_address,
        NULL :: text AS from_label_type,
        NULL :: text AS from_label_subtype,
        NULL :: text AS from_label,
        NULL :: text AS from_address_name,
        NULL :: text AS to_address,
        NULL :: text AS to_label_type,
        NULL :: text AS to_label_subtype,
        NULL :: text AS to_label,
        NULL :: text AS to_address_name,
        COALESCE(
            decoded_logs.method,
            event_name
        ) AS event_name,
        decoded_logs.type AS event_type,
        event_id,
        contract_address,
        e.symbol,
        NULL :: FLOAT AS amount,
        NULL :: FLOAT AS amount_usd
    FROM
        full_events e
        LEFT OUTER JOIN {{ source(
            'ethereum',
            'ethereum_decoded_log_methods'
        ) }} AS decoded_logs
        ON e.event_name = decoded_logs.encoded_log_method
    WHERE
        event_name IS NOT NULL
        AND event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
)
SELECT
    *
FROM
    token_transfers
UNION ALL
SELECT
    *
FROM
    eth_transfers
UNION ALL
SELECT
    *
FROM
    logs
ORDER BY
    block_timestamp
