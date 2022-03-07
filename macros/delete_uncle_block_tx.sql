{% macro delete_uncle_block_tx() -%}

CREATE TABLE IF NOT EXISTS {{ this.database }}.{{ this.schema }}._log_uncle_block_tx (
    block_id number,
    tx_id string,
    _inserted_timestamp timestamp_ntz default sysdate()
);

MERGE INTO {{ this.database }}.{{ this.schema }}._log_uncle_block_tx l USING (
    SELECT
        DISTINCT block_id,
        tx_id
    FROM
        {{ this }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - 7 
    qualify(RANK() over (PARTITION BY tx_id ORDER BY block_id DESC)) > 1
) s
ON l.block_id = s.block_id and l.tx_id = s.tx_id
WHEN NOT MATCHED THEN
    INSERT (
        block_id, 
        tx_id
    ) 
    VALUES (
        s.block_id, 
        s.tx_id
    );


DELETE FROM {{ this }} m USING (
    SELECT
        block_id,
        tx_id
    FROM
        {{ this.database }}.{{ this.schema }}._log_uncle_block_tx
    WHERE
        _inserted_timestamp >= CURRENT_DATE - 1
) ub
WHERE
    m.block_id = ub.block_id
    AND m.tx_id = ub.tx_id
    AND m.block_timestamp :: DATE >= CURRENT_DATE - 7;


{%- endmacro %}
