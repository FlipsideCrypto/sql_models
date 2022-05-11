{{ config(error_if = '>10', warn_if = '>1') }}

SELECT DISTINCT
    block_id
FROM {{ ref('silver_terra__msg_events') }}
WHERE block_id NOT IN (
    SELECT DISTINCT
        block_id 
    FROM {{ ref('terra__event_actions') }}
    WHERE block_timestamp::date >= '2021-10-01'
)

AND block_timestamp::date >= '2021-10-01'
AND tx_status = 'SUCCEEDED'
AND msg_type = 'wasm/MsgExecuteContract'
AND event_type = 'from_contract' 