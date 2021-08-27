{{ config(
    materialized = 'incremental',
    sort = ['date', 'currency'],
    unique_key = 'date',
    tags = ['balances']
) }}

WITH prices AS (

    SELECT
        p.symbol,
        DATE_TRUNC(
            'day',
            HOUR
        ) AS DAY,
        AVG(price) AS price
    FROM
        {{ ref('terra_prices') }}
        p
    WHERE

{% if is_incremental() %}
HOUR >= getdate() - INTERVAL '3 days'
{% else %}
    HOUR >= getdate() - INTERVAL '12 months'
{% endif %}
GROUP BY
    p.symbol,
    DAY
)
SELECT
    DATE,
    b.address,
    address_labels.l1_label AS address_label_type,
    address_labels.l2_label AS address_label_subtype,
    address_labels.project_name AS address_label,
    address_labels.address_name AS address_address_name,
    balance,
    balance * p.price AS balance_usd,
    b.balance_type,
    currency
FROM
    {{ source(
        'terra',
        'udm_daily_balances_terra'
    ) }}
    b
    LEFT OUTER JOIN prices p
    ON p.symbol = b.currency
    AND p.day = b.date
    LEFT OUTER JOIN {{ ref('terra_address_labels') }} AS address_labels
    ON b.address = address_labels.address
WHERE

{% if is_incremental() %}
DATE >= getdate() - INTERVAL '3 days'
{% else %}
    DATE >= getdate() - INTERVAL '12 months'
{% endif %}
