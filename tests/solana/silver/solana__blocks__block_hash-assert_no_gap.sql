WITH tmp AS (
SELECT block_hash, 
        previous_block_hash
FROM {{ ref('silver_solana__blocks') }}
)

SELECT t.block_hash 
FROM tmp t
LEFT JOIN tmp t2
ON t.previous_block_hash = t2.block_hash
WHERE t2.block_hash is null 
