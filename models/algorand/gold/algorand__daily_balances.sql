{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', date, address)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['date']
) }}

SELECT
    address,
    DATE,
    balance
FROM
    {{ ref('silver_algorand__daily_balances') }}
WHERE
    balance > 0

{% if is_incremental() %}
AND DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
