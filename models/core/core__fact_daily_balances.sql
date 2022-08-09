{{ config(
    materialized = 'incremental',
    unique_key = "fact_daily_balance_id",
    incremental_strategy = 'merge',
    cluster_by = ['date'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['a.address','date']
    ) }} AS fact_daily_balance_id,
    b.dim_account_id,
    A.address,
    DATE,
    balance
FROM
    {{ ref('silver__daily_balances') }} A
    JOIN {{ ref('core__dim_account') }}
    b
    ON A.address = b.address
WHERE
    balance > 0

{% if is_incremental() %}
AND DATE >=(
    SELECT
        DATEADD('day', -2, MAX(DATE))
    FROM
        {{ this }})
    {% endif %}
