{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, coalesce(event_id,-1))",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp', 'block_id'],
    tags = ['snowflake', 'polygon', 'polygon_udm_events_gold', 'address_labels']
) }}

WITH token_prices AS (

    select symbol,
    hour,
    token_address,
    price
    from {{ ref('silver_polygon__prices') }} 
    WHERE 1=1
    {% if is_incremental() %}
    AND hour::date >= (select max(block_timestamp::date) from {{ this }})
    {% endif %}
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
    AND recorded_at >= (select max(block_timestamp::date) from {{ this }})
  {% endif %}
  group by 1,2
  ),
poly_labels AS (
    SELECT
        l1_label,
        l2_label,
        project_name,
        address_name, 
        address
    FROM
        {{ ref('silver_crosschain__address_labels') }}
    WHERE
        blockchain = 'polygon'
),
base_udm_events AS (
    SELECT
        *
    FROM
        {{ ref('silver_polygon__udm_events')}}
    where 1=1

    {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ this }})
    {% endif %}
),
base_tx AS (
    SELECT
        *
    FROM
        {{ ref('silver_polygon__transactions')}}
    WHERE 1=1

    {% if is_incremental() %}
    AND block_timestamp::date >= (select max(block_timestamp::date) from {{ this }})
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
        LEFT OUTER JOIN {{ source('ethereum','sha256_function_signatures') }} AS f
        ON t.input_method = f.hex_signature
        AND f.importance = 1
        LEFT OUTER JOIN poly_labels AS from_labels
        ON t.from_address = from_labels.address
)
    select block_timestamp,
        block_id,
        e.tx_id,
        o.origin_address,
        o.origin_label_type,
        o.origin_label_subtype,
        o.origin_label,
        o.origin_address_name,
        o.origin_function_signature,
        o.origin_function_name,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else from_address end as from_address,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else from_label_type end as from_label_type,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else from_label_subtype end as from_label_subtype,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else from_label end as from_label,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else from_address_name end as from_address_name,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else to_address end as to_address,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else to_label_type end as to_label_type,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else to_label_subtype end as to_label_subtype,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else to_label end as to_label,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::string else to_address_name end as to_address_name,
        case when e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' or token_value > 0 then 'transfer' 
             when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then coalesce(d.method, event_name)
             else event_name end as event_name,
        case when e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then 'erc20_transfer' 
             when native_value > 0 then 'native_matic'
             when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then d.type
             else event_type end as event_type,
        event_id,
        contract_address,
        case when e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then coalesce(e.symbol, p.symbol) 
             when native_value > 0 then 'MATIC'
             else e.symbol end as symbol,
        -- native_value,
        case when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::float 
             when native_value > 0 then native_value else token_value/pow(10,de.decimal) end amount,
        case  when event_name is not null and event_name != '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then null::float
             when e.event_name = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then token_value * p.price / pow(10,de.decimal)
             when native_value > 0 then native_value * pp.price
             else null end as amount_usd
  from events e
  join originator o on e.tx_id = o.tx_id
  left join token_prices p on p.token_address = e.contract_address
                          and date_trunc('hour', e.block_timestamp) = p.hour
  left join poly_prices pp on date_trunc('hour', e.block_timestamp) = pp.hour
  left join {{ source('ethereum','ethereum_decoded_log_methods') }} d on e.event_name = d.encoded_log_method
  left join {{ ref('polygon_dbt__decimals')}} de on lower(de.token_id) = lower(e.contract_address)