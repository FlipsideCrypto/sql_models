# DOOR (Do Our Own Research) - SQL Models

This repository contains [DBT](https://docs.getdbt.com/docs/introduction) SQL Models that glue together and transform low-level tables into analytics ready data sets. The tables that are output from this DBT project are available to Flipside users [here in Velocity](https://app.flipsidecrypto.com/auth/signup/velocity)

The framework used to manage SQL table composition is [DBT](https://docs.getdbt.com/docs/introduction) and we use [Snowflake](https://docs.snowflake.com/en/index.html) as our Data Warehouse.

Project Docs: [sql-models.lab3547.com](http://sql-models.lab3547.com/#!/overview)

## Upgrading to DBT Version 1.0 for Local Development
We are now using DBT version 1.0 for this project.  Follow the instructions below for your operating system to upgrade your local development environment.

**MacOS**
If you originally installed dbt using homebrew, upgrade it using the below command
```
brew update
brew upgrade dbt-snowflake
brew link --overwrite dbt-snowflake
```

**Windows**
```
pip install --upgrade dbt-core
pip install --upgrade dbt-snowflake
```
After running the upgrade commands, run `dbt --versions` to check you are now on v1.0

For both systems you will need to re-run your dbt dependencies which will be installed in dbt_packages instead of dbt_modules

```
dbt deps
```

## Getting Started

**Step1**: Install the DBT command line tool on your local machine. Instructions [here at DBT](https://docs.getdbt.com/dbt-cli/installation).

**Step2**: Configure your "profile" a.k.a the credentials that DBT will use locally to connect to Snowflake.
Open a bash terminal and run `nano ~/.dbt/profiles.yml`. Copy and paste the below code in with your credentials that were provided by Flipside (replacing the variables in "<>" below).

```yml
snowflake:
    target: dev
    outputs:
        dev:
            type: snowflake
            account: <account>
            user: <username>
            password: <password>
            role: <role>
            region: <region>
            database: <database>
            warehouse: <warehouse>
            schema: <schema>
            threads: 4
            client_session_keep_alive: False
            query_tag: dbt
    config:
        send_anonymous_usage_stats: False
```

**Step3**: Test your config is working properly by running opening a bash terming in the directory of this project and running: `dbt run --models +tag:uniswapv3`. This command, if working properly, will do an incremental refresh of the uniswapv3 models found in `models/uniswapv3/`.

## Conventions

### File Names

We use two underscores in our model filenames `__` to denote the schema and table. For example `{schema}__{tablename}.sql`. We have configured a custom DBT macro that will split the filenames in this manner when creating the actual tables in snowflake. If the file name is `ethereum__transactions.sql`, the schema in snowflake will be `ethereum` and the table name will be `transactions`, queryable at `ethereum.transactions`.

### Contributing

If you would like to contribute a new model/table to our [Velocity](https://app.flipsidecrypto.com/auth/signup/velocity) product, please create a new branch, and once you are ready create a pull request with the following information: the goal of the PR, and the tables that will be produced by the PR. After submitting the PR a member of our analytics team will review it. If approved the models you produce here will be made available for querying in Flipside's [Velocity](https://app.flipsidecrypto.com/auth/signup/velocity) product.

### Source Data

Source tables that can be used when building models are outlined here: `models/sources.yml`. The majority of tables you will use as source for your models can be found in Flipside's `silver` schema.

### Supported Extensions

Extensions provide predefined blocks, functions, tests.

- [dbt expectations](https://github.com/calogica/dbt-expectations)
- [dbt utils](https://github.com/dbt-labs/dbt-utils)
- [dbt date](https://github.com/calogica/dbt-date)

## Commands

Install extensions. This only needs to be run once.

```sh
dbt deps
```

Run a model by matching on tag. In this case let's run all UniswapV3 models:

```sh
dbt run --models +tag:uniswapv3

```

Now let's do a full refresh of uniswapv3.

```sh
dbt run --models +tag:uniswapv3 --full-refresh
```

Run DBT tests

```sh
dbt test
```

### Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Sign-up for [Flipside Velocity](https://app.flipsidecrypto.com/auth/signup/velocity)
- Questions on how to use repo or get started? Join our [Discord]()
