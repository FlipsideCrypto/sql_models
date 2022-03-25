{{ config(
    materialized = 'incremental',
    sort = ['date', 'currency'],
    unique_key = 'date',
    tags = ['snowflake', 'terra_gold', 'terra_daily_balances']
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
        {{ ref('gold_terra__prices') }}
        p
    WHERE
        TRUE

{% if is_incremental() %}
AND HOUR >= getdate() - INTERVAL '3 days'
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
    LEFT OUTER JOIN {{ ref('gold_terra__address_labels') }}
    address_labels
    ON b.address = address_labels.address
WHERE
    TRUE

{% if is_incremental() %}
AND DATE >= getdate() - INTERVAL '3 days'
{% endif %}
