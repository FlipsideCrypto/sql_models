use role accountadmin;
-- Grant Compound to Velocity
grant usage
on schema flipside_prod_db.compound to role velocity_ethereum;
grant
select
    on all tables in schema flipside_prod_db.compound to role velocity_ethereum;
grant
select
    on future tables in schema flipside_prod_db.compound to role velocity_ethereum;
grant usage
    on schema flipside_prod_db.compound to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.compound to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.compound to role velocity_internal;
-- Grant UniswapV3 to Velocity
    grant usage
    on schema flipside_prod_db.uniswapv3 to role velocity_ethereum;
grant
select
    on all tables in schema flipside_prod_db.uniswapv3 to role velocity_ethereum;
grant
select
    on future tables in schema flipside_prod_db.uniswapv3 to role velocity_ethereum;
grant usage
    on schema flipside_prod_db.uniswapv3 to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.uniswapv3 to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.uniswapv3 to role velocity_internal;
-- Grant Ethereum to Velocity
    grant usage
    on schema flipside_prod_db.ethereum to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.ethereum to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.ethereum to role velocity_internal;
grant usage
    on schema flipside_prod_db.terra to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.terra to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.terra to role velocity_internal;
grant usage
    on schema flipside_prod_db.aave to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.aave to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.aave to role velocity_internal;
-- Grant DOOR_PROD role (used by DBT Cloud, and DOOR)
    GRANT ALL privileges
    on all schemas in database flipside_prod_db to role door_prod;
GRANT ALL privileges
    on future schemas in database flipside_prod_db to role door_prod;
GRANT ALL privileges
    on all tables in database flipside_prod_db to role door_prod;
GRANT ALL privileges
    on future tables in database flipside_prod_db to role door_prod;
-- Grant Compound to Velocity
    grant usage
    on schema flipside_prod_db.compound to role velocity_ethereum;
grant
select
    on all tables in schema flipside_prod_db.compound to role velocity_ethereum;
grant
select
    on future tables in schema flipside_prod_db.compound to role velocity_ethereum;
grant usage
    on schema flipside_prod_db.compound to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.compound to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.compound to role velocity_internal;
-- Grant UniswapV3 to Velocity
    grant usage
    on schema flipside_prod_db.uniswapv3 to role velocity_ethereum;
grant
select
    on all tables in schema flipside_prod_db.uniswapv3 to role velocity_ethereum;
grant
select
    on future tables in schema flipside_prod_db.uniswapv3 to role velocity_ethereum;
grant usage
    on schema flipside_prod_db.uniswapv3 to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.uniswapv3 to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.uniswapv3 to role velocity_internal;
-- Grant Ethereum to Velocity
    grant usage
    on schema flipside_prod_db.ethereum to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.ethereum to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.ethereum to role velocity_internal;
grant usage
    on schema flipside_prod_db.terra to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.terra to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.terra to role velocity_internal;
grant usage
    on schema flipside_prod_db.aave to role velocity_internal;
grant
select
    on all tables in schema flipside_prod_db.aave to role velocity_internal;
grant
select
    on future tables in schema flipside_prod_db.aave to role velocity_internal;
-- Grant DOOR_PROD role (used by DBT Cloud, and DOOR)
    GRANT ALL privileges
    on all schemas in database flipside_prod_db to role door_prod;
GRANT ALL privileges
    on future schemas in database flipside_prod_db to role door_prod;
GRANT ALL privileges
    on all tables in database flipside_prod_db to role door_prod;
GRANT ALL privileges
    on future tables in database flipside_prod_db to role door_prod;
GRANT ALL privileges
    on all schemas in database flipside_dev_db to role internal_dev;
GRANT ALL privileges
    on future schemas in database flipside_dev_db to role internal_dev;
GRANT ALL privileges
    on all tables in database flipside_dev_db to role internal_dev;
GRANT ALL privileges
    on future tables in database flipside_dev_db to role internal_dev;
