{{
  config(
    materialized='incremental',
    sort='block_id',
    unique_key='tx_id || voter',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_votes']
  )
}}




SELECT block_id,
       block_timestamp,
       contract_address AS governance_contract,
       event_inputs:id::STRING AS proposal_id,
       event_inputs:support::STRING AS support,
       event_inputs:votingPower::STRING AS voting_power,
       LOWER(event_inputs:voter::STRING) AS voter,
       tx_id
FROM {{ ref('ethereum__events_emitted') }}
WHERE 
event_name = 'VoteEmitted' AND contract_address = '0xec568fffba86c094cf06b22134b23074dfe2252c'