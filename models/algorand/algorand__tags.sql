{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='address',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'algorand', 'events']
  )
}}


SELECT sq.address,
        'top-total-balance-holder' AS name,
        'Top Holder' AS label
FROM (
    SELECT
    distinct sq_inner.address
    FROM (
    SELECT
        blockchain,
        date,
        address,
        balance,
        currency,
        row_number() over(partition by blockchain, address order by date desc) as rn
    FROM {{ source('algorand', 'udm_daily_balances_algorand')}} udb
    WHERE 
        blockchain = 'algorand'
        -- Threshold check
        AND balance >= 275000

        -- Remove exchanges
        AND address NOT IN (
            SELECT address FROM {{ source('shared', 'udm_address_labels_new')}} WHERE blockchain = 'algorand' AND l1_label = 'cex'
        )
        -- Remove foundation
        AND address NOT IN (
            SELECT address FROM {{ source('shared', 'udm_address_labels_new')}} WHERE blockchain = 'algorand' AND l1_label = 'chadmin'
        )
        -- Remove foundation
        AND address NOT IN (
            SELECT address FROM {{ source('shared', 'udm_address_labels_new')}} WHERE blockchain = 'algorand' AND l1_label = 'operator'
        )
        AND date >= GETDATE() - interval'1 month'
    ) sq_inner
    WHERE rn = 1
) sq