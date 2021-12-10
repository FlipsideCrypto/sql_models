{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', metric_date, swap_pair)",
  tags = ['snowflake', 'terra', 'terraswap', 'console', 'swap_spread']
) }}

SELECT metric_date,
       swap_pair, 
       sum(swap_fee_usd)/sum(to_value_usd) as metric_value
FROM
(SELECT date_trunc('day', block_timestamp) as metric_date,
       offer_currency as from_currency,
       offer_amount_usd as from_value_usd,
       ask_currency as to_currency,
       token_1_amount_usd as to_value_usd,
       swap_fee_amount_usd as swap_fee_usd,
       swap_pair
FROM terra.swaps
where metric_date >= current_date - 180
and from_currency IN('KRT', 'LUNA', 'UST', 'SDT')
and to_currency IN('KRT', 'LUNA', 'UST', 'SDT')
and swap_pair is not null
)
GROUP BY 1, 2
ORDER BY 1 asc