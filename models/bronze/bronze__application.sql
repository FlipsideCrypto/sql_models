{{ config (
    materialized = 'view'
) }}

SELECT
    INDEX,
    creator,
    deleted,
    created_at,
    closed_at,
    params,
    __HEVO__LOADED_AT,
    DATEADD(
        ms,
        __HEVO__LOADED_AT,
        '1970-01-01'
    ) AS _inserted_timestamp
FROM
    {{ source(
        'algorand',
        'APP'
    ) }}
