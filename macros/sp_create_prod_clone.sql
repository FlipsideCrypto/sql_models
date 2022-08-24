{% macro sp_create_prod_clone(target_schema) -%}

create or replace procedure {{ target_schema }}.create_prod_clone(source_db_name string, destination_db_name string, role_name string)
returns boolean 
language javascript
execute as caller
as
$$
    snowflake.execute({sqlText: `BEGIN TRANSACTION;`});
    try {
        snowflake.execute({sqlText: `DROP DATABASE IF EXISTS ${DESTINATION_DB_NAME}`});
        snowflake.execute({sqlText: `CREATE DATABASE ${DESTINATION_DB_NAME} CLONE ${SOURCE_DB_NAME}`});
        snowflake.execute({sqlText: `DROP SCHEMA ${DESTINATION_DB_NAME}._INTERNAL`}); /* this only needs to be in prod */

        var existing_schemas = snowflake.execute({sqlText: `SELECT table_schema
            FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.TABLE_PRIVILEGES
            WHERE grantor IS NOT NULL
            GROUP BY 1;`});

        while (existing_schemas.next()) {
            var schema = existing_schemas.getColumnValue(1)
            snowflake.execute({sqlText: `GRANT OWNERSHIP ON SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`});
        }

        var existing_tables = snowflake.execute({sqlText: `SELECT table_schema, table_name
            FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.TABLE_PRIVILEGES
            WHERE grantor IS NOT NULL
            GROUP BY 1,2;`});

        while (existing_tables.next()) {
            var schema = existing_tables.getColumnValue(1)
            var table_name = existing_tables.getColumnValue(2)
            snowflake.execute({sqlText: `GRANT OWNERSHIP ON TABLE ${DESTINATION_DB_NAME}.${schema}.${table_name} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`});
        }

        var existing_functions = snowflake.execute({sqlText: `SELECT function_schema, function_name, concat('(',array_to_string(regexp_substr_all(argument_signature, 'VARCHAR|NUMBER|FLOAT|ARRAY|VARIANT|OBJECT|DOUBLE'),','),')') as argument_signature
            FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.FUNCTIONS;`});

        while (existing_functions.next()) {
            var schema = existing_functions.getColumnValue(1)
            var function_name = existing_functions.getColumnValue(2)
            var argument_signature = existing_functions.getColumnValue(3)
            snowflake.execute({sqlText: `GRANT OWNERSHIP ON FUNCTION ${DESTINATION_DB_NAME}.${schema}.${function_name}${argument_signature} to role ${ROLE_NAME} REVOKE CURRENT GRANTS;`});
        }

        var existing_procedures = snowflake.execute({sqlText: `SELECT procedure_schema, procedure_name, concat('(',array_to_string(regexp_substr_all(argument_signature, 'VARCHAR|NUMBER|FLOAT|ARRAY|VARIANT|OBJECT|DOUBLE'),','),')') as argument_signature
            FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.PROCEDURES;`});

        while (existing_procedures.next()) {
            var schema = existing_procedures.getColumnValue(1)
            var procedure_name = existing_procedures.getColumnValue(2)
            var argument_signature = existing_procedures.getColumnValue(3)
            snowflake.execute({sqlText: `GRANT OWNERSHIP ON PROCEDURE ${DESTINATION_DB_NAME}.${schema}.${procedure_name}${argument_signature} to role ${ROLE_NAME} REVOKE CURRENT GRANTS;`});
        }

        snowflake.execute({sqlText: `GRANT OWNERSHIP ON DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME};`})
        snowflake.execute({sqlText: `COMMIT;`});
    } catch (err) {
        snowflake.execute({sqlText: `ROLLBACK;`});
        throw(err);
    }
    
    return true
$$

{%- endmacro %}