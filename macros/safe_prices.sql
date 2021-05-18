{% macro safe_ethereum_prices(eth_price_table, incremental_days = '30 days', full_refresh_days = '9 months') -%}

    SELECT
        date_trunc('day', sq_outer.hour) as hour,
        lower(sq_outer.token_address) as token_address,
        sq_outer.decimals,
        avg(sq_outer.price) as price
    FROM (
        SELECT
            *
        FROM
        (
            SELECT 
                *,
                row_number() OVER (PARTITION BY token_address, hour ORDER BY hour DESC) AS rn
            FROM {{ eth_price_table }}
            WHERE 
            price IS NOT NULL
            AND 
            {% if is_incremental() %}
            hour >= getdate() - interval '{{incremental_days}}'
            {% else %}
            hour >= getdate() - interval '{{full_refresh_days}}'
            {% endif %}
        ) sq
        WHERE 
        sq.rn = 1
    ) sq_outer
    GROUP BY 1,2,3

{%- endmacro %}