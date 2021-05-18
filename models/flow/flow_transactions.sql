{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='tx_id',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'flow', 'events']
  )
}}

SELECT
  block_timestamp,
  tx_id,
  CASE
    -- transfers
    WHEN tx_type = 'tokens_deposited | tokens_withdrawn' THEN 'token_transfer'
    WHEN tx_type = 'deposit | withdraw' THEN 'nft_transfer'

    -- nft lifecycle behavior
    WHEN 
      tx_type LIKE '%moment_minted%' 
      OR tx_type LIKE '%token_minted%' 
      OR tx_type LIKE '%nft_minted%' 
    THEN 'nft_minted'
    WHEN 
      tx_type LIKE '%moment_listed%' 
      OR tx_type LIKE '%token_listed%' 
      OR tx_type LIKE '%nft_listed%' 
    THEN 'nft_listed'
    WHEN 
      tx_type LIKE '%moment_withdrawn%' 
      OR tx_type LIKE '%token_withdrawn%' 
      OR tx_type LIKE '%nft_withdrawn%' 
    THEN 'nft_withdrawn'
    WHEN 
      tx_type LIKE '%moment_purchased%' 
      OR tx_type LIKE '%token_purchased%' 
      OR tx_type LIKE '%nft_purchased%' 
    THEN 'nft_purchased'
    
    -- tokens
    WHEN tx_type = 'tokens_price_changed' THEN 'nft_price_changed'
    
    -- nft
    WHEN tx_type = 'nft_data_updated' THEN 'nft_data_updated'

    -- play
    WHEN tx_type = 'play_created' THEN 'play_created'
    WHEN tx_type = 'play_added_to_set' THEN 'play_added_to_set'
    WHEN tx_type = 'play_retired_from_set' THEN 'play_retired_from_set'
    
    -- set
    WHEN tx_type = 'set_created' THEN 'set_created'
    
    -- items
    WHEN tx_type = 'item_created' THEN 'item_created'
    WHEN tx_type = 'item_added_to_set' THEN 'item_added_to_set'

    -- collectible
    WHEN tx_type LIKE '%collectible_minted%' THEN 'collectible_minted'
    
    -- minter
    WHEN tx_type LIKE '%minter_created%' THEN 'minter_created'
    
    -- series
    WHEN tx_type = 'new_series_started' THEN 'new_series_started'

    -- staking related
    WHEN 
      tx_type LIKE 'delegator_unstaked_tokens_withdrawns' 
    THEN 'delegator_unstaked_tokens_withdrawn'
    
    -- delegator tokens
    WHEN tx_type = 'delegator_tokens_committed' THEN 'delegator_tokens_committed'
    WHEN tx_type = 'delegator_tokens_unstaked' THEN 'delegator_tokens_unstaked'
    WHEN tx_type = 'delegator_tokens_staked' THEN 'delegator_tokens_staked'

    -- tokens
    WHEN tx_type = 'tokens_staked' THEN 'tokens_staked'
    WHEN tx_type = 'tokens_unstaked' THEN 'tokens_unstaked'
    WHEN tx_type LIKE '%tokens_minted%' THEN 'tokens_minted'
    WHEN tx_type LIKE '%tokens_burned%' THEN 'tokens_burned'

    -- rewards
    WHEN tx_type LIKE '%reward_tokens_withdrawn%' THEN 'reward_tokens_withdrawn'
    WHEN tx_type LIKE '%unstaked_tokens_withdrawn%' THEN 'unstaked_tokens_withdrawn'

    -- node
    WHEN tx_type LIKE '%new_node_created%' THEN 'new_node_created'
    WHEN tx_type LIKE '%node_removed_and_refunded%' THEN 'node_removed_and_refunded'  

    -- epoch
    WHEN tx_type LIKE '%new_epoch%' THEN 'new_epoch'
    
    -- delegator
    WHEN tx_type LIKE '%new_delegator_cut_percentage%' THEN 'unstaked_tokens_withdrawn'
    WHEN tx_type LIKE '%rewards_paid%' THEN 'rewards_paid'

    -- account/contract related
    WHEN tx_type = 'account_key_added' THEN 'account_key_added'
    WHEN tx_type LIKE '%account_created%' THEN 'account_created'
    WHEN tx_type LIKE '%contract_initialized%' THEN 'contract_initialized'

    -- catch all for single-type events like unlock_limit_increased
    WHEN tx_type NOT LIKE '%|%' THEN tx_type

  ELSE 'not_classified' END as tx_type
FROM (
  SELECT
    block_timestamp,
    tx_id,
    listagg(distinct event_type, ' | ') within group(order by event_type) as tx_type
  FROM
    {{ source('flow', 'udm_events_flow')}}
  WHERE
    {% if is_incremental() %}
      block_timestamp >= getdate() - interval '7 days'
    {% else %}
      block_timestamp >= getdate() - interval '9 months'
    {% endif %}
  GROUP BY block_timestamp, tx_id
)
