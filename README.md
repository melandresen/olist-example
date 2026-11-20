# Olist ELT Pipeline: Airflow + Snowflake + dbt

Airflow loads the [Olist Kaggle dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) into Snowflake, then runs `dbt build`.

## Prerequisites

- Docker (at least 4 GB of memory)
- A Snowflake account with database `SNOWFLAKE_LEARNING_DB`, warehouse `COMPUTE_WH` and role `ACCOUNTADMIN` (a trial account has these by default)

## Run it

1. **Get the data:** download the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and unzip the nine CSV files into `data/`.

2. **Create `.env`:**
   ```bash
   cp .env.example .env
   ```
   Fill in `AIRFLOW_UID` (from `id -u`) and your Snowflake account identifier, username and password.

3. **Start Airflow:**
   ```bash
   docker compose up -d
   ```
   The first start takes a few minutes while dbt gets installed. Then open http://localhost:8080.

4. **Add the Snowflake connection:** in Airflow, go to *Admin → Connections → Add Connection*:
   - Connection ID: `snowflake`
   - Connection Type: `Snowflake`
   - Login, Password, Account: your Snowflake credentials
   - Warehouse: `COMPUTE_WH`, Database: `SNOWFLAKE_LEARNING_DB`, Role: `ACCOUNTADMIN`

5. **Run:** unpause and trigger the DAG `olist_pipeline`.

The results end up in the schemas `OLIST` (raw), `OLIST_STAGING`, `OLIST_INTERMEDIATE` and `OLIST_MARTS`.
