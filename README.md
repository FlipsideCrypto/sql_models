# DOOR (Do Our Own Research) - SQL Models
This repository contains [DBT](https://docs.getdbt.com/docs/introduction) SQL Models that glue together and transform low-level tables into analytics ready data sets. The tables that are output from this DBT project are available to Flipside users [here in Velocity](https://app.flipsidecrypto.com/)

The framework used to manage SQL table composition is [DBT](https://docs.getdbt.com/docs/introduction) and we use [Snowflake](https://docs.snowflake.com/en/index.html) as our Data Warehouse.

Project Docs: [sql-models.flipsidecrypto.com](sql-models.flipsidecrypto.com)

## Commands

Run a model by matching on tag. In this case let's run all UniswapV3 models:
```
dbt run --models +tag:uniswapv3

```

Now let's do a full refresh of uniswapv3.
```
dbt run --models +tag:uniswapv3 --full-refresh
```

Run DBT tests
```
dbt test
```

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Sign-up for [Flipside Velocity](https://app.flipsidecrypto.com/)
