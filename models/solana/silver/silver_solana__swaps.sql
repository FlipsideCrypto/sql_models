{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert', 
    cluster_by = ['block_timestamp::DATE'],
    tags = ['snowflake', 'solana', 'silver_solana', 'solana_events', 'solana_swaps']
) }} 

WITH base_table AS (
  SELECT 
    block_timestamp, 
    block_id, 
    blockchain, 
    recent_blockhash, 
    tx_id,
    event_info[0]:parsed:info:source :: STRING AS sender,  
    
   CASE WHEN event_info[2]:programId :: STRING  IN ('9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin', '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8') -- SERUM DEX || Raydium Liquidity Pool V4
   THEN event_info[1]:parsed:info:mint :: STRING
   WHEN event_info[3]:programId :: STRING = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ' -- SABER STABLE SWAP
   THEN event_meta[1]:parsed:info:destination :: STRING
   ELSE COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING
        ) END AS token_sent_account,
        
   CASE WHEN event_info[3]:programId :: STRING IN ('SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ', 'SSwpMgqNDsyV7mAgN9ady4bDVu5ySjmmXejXvy2vLt1') -- SABER STABLE SWAP || STEP FINANCE
    THEN postTokenBalances[1]:mint :: STRING
    WHEN event_info[4]:programId :: STRING  = '82yxjeMsvaURa4MbZZ7WZZHfobirZYkH1zF8fmeGtyaQ' -- ORCA AQUAFARM
    THEN  postTokenBalances[3]:mint :: STRING
    WHEN event_info[2]:programId :: STRING  IN ('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP')-- ORCA TOKEN SWAP
    THEN postTokenBalances[0]:mint :: STRING
    WHEN event_info[2]:programId :: STRING IN ('2nAAsYdXF3eTQzaeUQS3fr4o782dDg8L28mX39Wr5j8N', '31ARfyxZg6fr1J9hVs1NqBWkdeYeCKipuY71bMovNpy9') -- UNKNOWN PROGRAM 
    THEN postTokenBalances[1]:mint :: STRING
    ELSE postTokenBalances[0]:mint :: STRING END AS t0_sent, 
    
    CASE WHEN event_info[2]:programId :: STRING = 'LendZqTs7gn5CTSJU1jWKhKuVpjJGom45nnwPb2AMTi'
    THEN event_meta[1]:parsed:info:amount :: INTEGER
    ELSE event_meta[0]:parsed:info:amount :: INTEGER END AS amount_sent, 
    
    CASE WHEN event_info[2]:programId :: STRING  IN ('9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin', '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8') -- SERUM DEX || Raydium Liquidity Pool V4
    THEN COALESCE(
        event_meta[0]:parsed:info:destination :: STRING, 
        event_meta[1]:parsed:info:destination :: STRING
        )
    WHEN event_info[3]:programId :: STRING  IN ('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP' , 'SWiMDJYFUGj6cPrQ6QYYYWZtvXQdRChSVAygDZDsCHC')-- ETH WORMHOLE
    THEN postTokenBalances[1]:mint :: STRING
    WHEN event_info[4]:programId :: STRING  = 'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky' -- Mercurial Stable Swap
    THEN postTokenBalances[3]:mint :: STRING
    WHEN event_info[3]:programId :: STRING  = 'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky' -- Mercurial Stable Swap
    THEN postTokenBalances[2]:mint :: STRING
    WHEN event_info[4]:programId :: STRING  = '82yxjeMsvaURa4MbZZ7WZZHfobirZYkH1zF8fmeGtyaQ' -- ORCA AQUAFARM
    THEN postTokenBalances[0]:mint :: STRING
    WHEN event_info[2]:programId :: STRING  IN ('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP','SSwpMgqNDsyV7mAgN9ady4bDVu5ySjmmXejXvy2vLt1', 'DmzAmomATKpNp2rCBfYLS7CSwQqeQTsgRYJA1oSSAJaP', '31ARfyxZg6fr1J9hVs1NqBWkdeYeCKipuY71bMovNpy9', 'Bt2WPMmbwHPk36i4CRucNDyLcmoGdC7xEdrVuxgJaNE6', 'PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP', 'SWiMDJYFUGj6cPrQ6QYYYWZtvXQdRChSVAygDZDsCHC')  -- ORCA TOKEN SWAP V2 || STEP FINANCE || UNKNOWN PROGRAM 
    THEN postTokenBalances[1]:mint :: STRING
    WHEN event_info[3]:programId :: STRING IN ('DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1', '2rcQtPTFZd3UWzEWwa86TtYn3ePi4U2EDaZxf3BKSvfd') -- ORCA TOKEN SWAP V1 || UNKNOWN
    THEN postTokenBalances[1]:mint :: STRING
    WHEN event_info[2]:programId :: STRING = '2nAAsYdXF3eTQzaeUQS3fr4o782dDg8L28mX39Wr5j8N' -- UNKNOWN PROGRAM 
    THEN postTokenBalances[0]:mint :: STRING
    WHEN event_info[2]:programId :: STRING = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
    THEN COALESCE (
       event_info[1]:parsed:info:mint :: STRING,
       postTokenBalances[3]:mint :: STRING 
    )
    WHEN event_info[2]:programId :: STRING = 'LendZqTs7gn5CTSJU1jWKhKuVpjJGom45nnwPb2AMTi'
    THEN postTokenBalances[4]:mint :: STRING
    ELSE event_info[1]:parsed:info:mint :: STRING END AS token_received,
    
    CASE WHEN event_info[2]:programId :: STRING IN ('SSwpMgqNDsyV7mAgN9ady4bDVu5ySjmmXejXvy2vLt1', 'PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP')
    THEN event_meta[1]:parsed:info:amount :: INTEGER 
    WHEN event_info[4]:programId :: STRING  = 'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky' -- Mercurial Stable Swap
    THEN event_info[3]:parsed:info:amount :: INTEGER
    WHEN event_info[3]:programId :: STRING  = 'SWiMDJYFUGj6cPrQ6QYYYWZtvXQdRChSVAygDZDsCHC'
    THEN event_meta[1]:parsed:info:amount :: INTEGER 
    WHEN event_meta[2]:parsed:type :: STRING <> 'burn'
    THEN COALESCE(
    event_meta[2]:parsed:info:amount :: INTEGER,
    event_meta[1]:parsed:info:amount :: INTEGER 
    ) 
    ELSE event_meta[1]:parsed:info:amount :: INTEGER 
    END AS amount_received, 
  
    succeeded, 
    ingested_at
        
FROM  "FLIPSIDE_DEV_DB"."SILVER_SOLANA"."EVENTS"

WHERE event_info[1]:program = 'spl-token' 
AND event_info[1]:programId = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
AND event_meta[0]:parsed:info:amount :: INTEGER IS NOT NULL -- IF THESE ARE NULL, THEN NFT MINT
AND event_meta[1]:parsed:info:amount :: INTEGER IS NOT NULL
AND token_received IS NOT NULL

{% if is_incremental() %}
  AND ingested_at >= (
    SELECT
      MAX(
        ingested_at
      )
    FROM
      {{ this }}
  )
{% endif %}
), 

token_account AS (
    SELECT 
        tx:transaction:message:instructions[1]:program :: STRING as program, 
        tx:transaction:message:instructions[1]:parsed:info:wallet :: STRING as owner_address, 
        tx:transaction:message:instructions[1]:parsed:info:account :: STRING as associated_token_address, 
        tx:transaction:message:instructions[1]:parsed:info:mint :: STRING as token_address, 
        ingested_at
    FROM {{ ref('bronze_solana__transactions') }}
    WHERE program = 'spl-associated-token-account'
    AND 
      1 = 1
    {% if is_incremental() %}
  AND ingested_at >= (
    SELECT
      MAX(
        ingested_at
      )
    FROM
      {{ this }}
  )
{% endif %}
), 

token_address_table AS (
  SELECT 
      block_timestamp, 
      block_id, 
      blockchain, 
      recent_blockhash, 
      tx_id,
      sender,
      token_sent_account,
      t0_sent,     
      t.token_address AS token_sent2,
      amount_sent, 
      token_received, 
      amount_received, 
      succeeded, 
      b.ingested_at
      
  FROM base_table b
  
  LEFT OUTER JOIN token_account t
  ON token_sent_account = t.associated_token_address
)

SELECT 
  block_timestamp, 
  block_id, 
  blockchain, 
  recent_blockhash, 
  tx_id, 
  sender,
  
  COALESCE (
    token_sent2, 
    t0_sent,
    token_sent_account) AS token_sent,
  
  CASE WHEN token_sent_account IN ('48F1neXh5bGgKr8G6CM6tFZkaC51UgtVb5pqGLC27Doi', 
          '2Zkvs84qBpMvWFSh9MFek2ZPAqAEjfufrs7Sbu42PbzBRTSv5Qx1bTWTGsbaSsuAPe4yx6ku3KmwVaGRbdANytF6',  
          'DD6oh3HRCvMzqHkGeUW3za4pLgWNPJdV6aNYW3gVjXXi', 
          'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz', 
          '9wArqjNDHm43RJGSBSXuHygqJzEaW1xYA3XmmpU671kC', 
          '5sHXydo4z6p2x7Jqfx8T4FnPRM9S2y8wpgNG1pAZf9Cg', 
          'ArwS8uBqFTKNgLAy7r3poSCujCazc4GiqBZguUzQPrKj', 
          '4aiKnDHFmNnLsopVsDyRBh8sbVohZYgdGzh3P9orpqNB', 
          '2YuVnXkSmW714ziXkfPfmvAnniBc1tHiAB7gYgRpx43a', 
          'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So', 
          'StepAscQoEioFxxWGnh2sLBDFp9d8rvKz2Yp39iDpyT',
          'So11111111111111111111111111111111111111112', 
          'DubwWZNWiNGMMeeQHPnMATNj77YZPZSAz2WVR5WjLJqz', 
          'H7Qc9APCWWGDVxGD5fJHmLTmdEgT9GFatAKFNg6sHh8A', 
          '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj') 
  THEN amount_sent/POW(10,9) :: INTEGER
  WHEN token_sent_account = '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs' 
  THEN amount_sent/POW(10,8) :: INTEGER
  WHEN token_sent IN ('48F1neXh5bGgKr8G6CM6tFZkaC51UgtVb5pqGLC27Doi', 
          '2Zkvs84qBpMvWFSh9MFek2ZPAqAEjfufrs7Sbu42PbzBRTSv5Qx1bTWTGsbaSsuAPe4yx6ku3KmwVaGRbdANytF6',  
          'DD6oh3HRCvMzqHkGeUW3za4pLgWNPJdV6aNYW3gVjXXi', 
          'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz', 
          '9wArqjNDHm43RJGSBSXuHygqJzEaW1xYA3XmmpU671kC', 
          '5sHXydo4z6p2x7Jqfx8T4FnPRM9S2y8wpgNG1pAZf9Cg', 
          'ArwS8uBqFTKNgLAy7r3poSCujCazc4GiqBZguUzQPrKj', 
          '4aiKnDHFmNnLsopVsDyRBh8sbVohZYgdGzh3P9orpqNB', 
          '2YuVnXkSmW714ziXkfPfmvAnniBc1tHiAB7gYgRpx43a', 
          'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So', 
          'StepAscQoEioFxxWGnh2sLBDFp9d8rvKz2Yp39iDpyT',
          'So11111111111111111111111111111111111111112',  
          'DubwWZNWiNGMMeeQHPnMATNj77YZPZSAz2WVR5WjLJqz', 
          'H7Qc9APCWWGDVxGD5fJHmLTmdEgT9GFatAKFNg6sHh8A', 
          '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj')
  THEN amount_sent/POW(10,9) :: INTEGER
  WHEN token_sent IN ('7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs', '9TE7ebz1dsFo1uQ2T4oYAKSm39Y6fWuHrd6Uk6XaiD16')
  THEN amount_sent/POW(10,8) :: INTEGER
  ELSE amount_sent/POW(10,6) :: INTEGER END AS amount_sent, 

  token_received,

  CASE WHEN token_received IN ('48F1neXh5bGgKr8G6CM6tFZkaC51UgtVb5pqGLC27Doi', 
          '2Zkvs84qBpMvWFSh9MFek2ZPAqAEjfufrs7Sbu42PbzBRTSv5Qx1bTWTGsbaSsuAPe4yx6ku3KmwVaGRbdANytF6',  
          'DD6oh3HRCvMzqHkGeUW3za4pLgWNPJdV6aNYW3gVjXXi', 
          'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz', 
          '9wArqjNDHm43RJGSBSXuHygqJzEaW1xYA3XmmpU671kC', 
          '5sHXydo4z6p2x7Jqfx8T4FnPRM9S2y8wpgNG1pAZf9Cg', 
          'ArwS8uBqFTKNgLAy7r3poSCujCazc4GiqBZguUzQPrKj', 
          '4aiKnDHFmNnLsopVsDyRBh8sbVohZYgdGzh3P9orpqNB', 
          '2YuVnXkSmW714ziXkfPfmvAnniBc1tHiAB7gYgRpx43a', 
          'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So', 
          'StepAscQoEioFxxWGnh2sLBDFp9d8rvKz2Yp39iDpyT',
          'So11111111111111111111111111111111111111112', 
          'AGFEad2et2ZJif9jaGpdMixQqvW5i81aBdvKe7PHNfz3', 
          'DubwWZNWiNGMMeeQHPnMATNj77YZPZSAz2WVR5WjLJqz') 
  THEN amount_received/POW(10,9) :: INTEGER
  WHEN token_received IN ('7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs', '9TE7ebz1dsFo1uQ2T4oYAKSm39Y6fWuHrd6Uk6XaiD16')
  THEN amount_received/POW(10,8) :: INTEGER
  ELSE amount_received/POW(10,6) :: INTEGER END AS amount_received, 
   
  succeeded, 
  ingested_at

FROM token_address_table

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
  ingested_at DESC)) = 1
