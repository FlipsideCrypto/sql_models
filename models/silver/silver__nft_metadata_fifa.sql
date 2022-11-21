{{ config(
    materialized = 'incremental',
    unique_key = 'nft_asset_id',
    incremental_strategy = 'merge',
    cluster_by = ['nft_asset_id']
) }}

WITH base AS (

    SELECT
        asset_id,
        asset_name,
        asset_URL,
        metadata_URL,
        metadata :animation_url :: STRING AS animation_url,
        metadata :animation_url_mimetype :: STRING AS animation_url_mimetype,
        metadata :description :: STRING AS description,
        metadata :external_url :: STRING AS external_url,
        metadata :external_url_mimetype :: STRING AS external_url_mimetype,
        metadata :image :: STRING AS image,
        metadata :image_integrity :: STRING AS image_integrity,
        metadata :image_mimetype :: STRING AS image_mimetype,
        metadata :name :: STRING AS NAME,
        metadata :properties :"arc-18" :"rekey-checked" :: STRING AS rekey_checked,
        metadata :properties :"arc-20" :"application-id" :: STRING AS application_id,
        RIGHT(LEFT(asset_name, 3), 2) :: INT AS drop_number,
        CASE
            drop_number
            WHEN 1 THEN 'genesis'
            WHEN 2 THEN 'archives'
            WHEN 3 THEN 'south american flair'
            WHEN 4 THEN 'archives 2'
        END drop_name,
        CASE
            WHEN description LIKE '%Womens World Cup%' THEN 'Womens'
            ELSE 'Mens'
        END AS world_cup_type,
        CASE
            WHEN world_cup_type = 'Mens' THEN SUBSTR(
                description,
                0,
                4
            )
            ELSE REGEXP_SUBSTR(
                description,
                '[1-2][0-9][0-9][0-9]'
            )
        END AS YEAR,
        CASE
            WHEN world_cup_type = 'Womens' THEN SUBSTR(
                description,
                22,
                REGEXP_INSTR(
                    description,
                    '™'
                ) -27
            )
            ELSE SUBSTR(
                description,
                20,
                REGEXP_INSTR(
                    description,
                    '™'
                ) -20
            )
        END AS host,
        SUBSTRING(description, REGEXP_INSTR(description, '-') -6, 13) AS teams,
        LEFT(
            teams,
            3
        ) AS country_1,
        SUBSTRING (
            teams,
            5,
            1
        ) AS country_1_score,
        RIGHT(
            teams,
            3
        ) AS country_2,
        SUBSTRING(
            teams,
            9,
            1
        ) AS country_2_score,
        SUBSTRING(description, REGEXP_INSTR(description, ', **') + 4, REGEXP_INSTR(description, ':') - REGEXP_INSTR(description, ', **') -4) AS event_type,
        SUBSTRING(description, REGEXP_INSTR(description, ':') + 2, REGEXP_INSTR(description, '[0-9]', REGEXP_INSTR(description, ':')) - (REGEXP_INSTR(description, ':') + 2)) AS player,
        REPLACE(
            SUBSTRING(
                description,
                REGEXP_INSTR(
                    description,
                    ' [0-9]',
                    REGEXP_INSTR(
                        description,
                        ':'
                    )
                ),
                10
            ),
            '**'
        ) AS MINUTE,
        SUBSTRING(asset_name, REGEXP_INSTR(asset_name, '-') + 1, 2) AS item_no,
        SUBSTRING(NAME, REGEXP_INSTR(NAME, '\\(') + 1, (REGEXP_INSTR(NAME, '\\)') - REGEXP_INSTR(NAME, '\\(') - 10)) AS editions,
        CASE
            drop_number
            WHEN 1 THEN CASE
                WHEN item_no < 3 THEN 'Iconic'
                WHEN item_no < 9 THEN 'Epic'
                WHEN item_no < 22 THEN 'Rare'
                ELSE 'Common'
            END
            WHEN 2 THEN CASE
                WHEN item_no < 2 THEN 'Iconic'
                WHEN item_no < 6 THEN 'Epic'
                WHEN item_no < 18 THEN 'Rare'
                ELSE 'Common'
            END
            WHEN 3 THEN CASE
                WHEN item_no < 4 THEN 'Iconic'
                WHEN item_no < 13 THEN 'Epic'
                WHEN item_no < 31 THEN 'Rare'
                ELSE 'Common'
            END
            WHEN 4 THEN CASE
                WHEN item_no < 2 THEN 'Iconic'
                WHEN item_no < 6 THEN 'Epic'
                WHEN item_no < 18 THEN 'Rare'
                ELSE 'Common'
            END
        END rarity
    FROM
        (
            SELECT
                asset_id,
                asset_name,
                asset_URL,
                metadata_URL,
                PARSE_JSON(metadata) AS metadata
            FROM
                bronze.fifa_metadata
            WHERE
                asset_name NOT LIKE 'test%'
        )
)
SELECT
    asset_id AS nft_asset_id,
    asset_name AS nft_asset_name,
    asset_URL AS nft_url,
    metadata_URL,
    animation_url,
    animation_url_mimetype,
    description,
    external_url,
    external_url_mimetype,
    image,
    image_integrity,
    image_mimetype,
    NAME,
    rekey_checked,
    application_id,
    drop_number,
    drop_name,
    item_no,
    editions,
    rarity,
    world_cup_type,
    YEAR,
    host,
    country_1,
    country_1_score,
    country_2,
    country_2_score,
    event_type,
    player,
    MINUTE
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY asset_id
ORDER BY
    NAME) = 1)
