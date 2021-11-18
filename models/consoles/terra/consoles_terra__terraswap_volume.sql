{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terraswap_volume']
) }}
-- queryId: 09c665d2-86d5-457d-babf-180880012d87
with prices as (
    SELECT
        date_trunc('day', block_timestamp) AS dayz,
        currency,
        avg(price_usd) * 1 as avgg
    FROM
       {{ ref('terra__oracle_prices') }}
    WHERE
        dayz >= CURRENT_DATE - 90
    group by
        dayz,
        currency
    order by
        dayz DESC
), nonnative as (
    SELECT
        date_trunc('day', block_timestamp) AS dayzz,
        COUNT(DISTINCT tx_id) AS n_trades,
        COUNT(DISTINCT (msg_value :sender :: string)) as n_traders,
        msg_value :execute_msg :swap :offer_asset :info :native_token :denom :: string as swap_currency,
        sum(
            msg_value :execute_msg :swap :offer_asset :amount / POW(10, 6)
        ) as trading_vol_token0,
        c.address as contract_label
    FROM
        {{ ref('terra__msgs') }}
        LEFT OUTER JOIN {{ ref('terra__labels') }} c ON msg_value :contract :: string = c.address
    WHERE
        msg_value :execute_msg :swap IS NOT NULL
        and contract_label is not NULL
  		and tx_status = 'SUCCEEDED'
    group by
        dayzz,
        swap_currency,
        contract_label
    order by
        dayzz DESC
), combine as (
    select
        *
    FROM
        nonnative
        left join prices on nonnative.dayzz = prices.dayz
        and nonnative.swap_currency = prices.currency
)
select
    dayzz,
    n_trades,
    n_traders,
    --currency,
    contract_label,
    (avgg * TRADING_VOL_TOKEN0) as USDADJ
from
    combine
where
    USDADJ > 0
    and dayz >= CURRENT_DATE - 90