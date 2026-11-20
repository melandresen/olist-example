import csv
from datetime import datetime
from pathlib import Path

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

FILES = [
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_customers_dataset.csv",
]

DATA_DIR = Path("/opt/airflow/data")
DATABASE = "SNOWFLAKE_LEARNING_DB"
SCHEMA = "OLIST"
FQ_SCHEMA = f"{DATABASE}.{SCHEMA}"
CONTROL_TABLE = f"{FQ_SCHEMA}.LOADED_FILES"
STAGE = f"{FQ_SCHEMA}.OLIST_STAGE"
FILE_FORMAT = f"{FQ_SCHEMA}.OLIST_CSV"


def read_headers(file_path: Path) -> list[str]:
    """Column names from the first line of the CSV (csv module handles quoting)."""
    # utf-8-sig strips the byte order mark some files start with
    # (product_category_name_translation.csv does)
    with file_path.open(encoding="utf-8-sig", newline="") as f:
        return [col.strip() for col in next(csv.reader(f))]


@dag(
    start_date=datetime(2026, 11, 15),
    catchup=False,
    schedule=None,
    tags=["snowflake", "olist", "etl"],
)
def olist_pipeline():

    @task
    def setup():
        SnowflakeHook(snowflake_conn_id="snowflake").run([
            f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA}",
            f"""CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
                    FILE_NAME STRING PRIMARY KEY,
                    LOAD_TIMESTAMP TIMESTAMP
                )""",
            # IF NOT EXISTS (not OR REPLACE): keeps the staged files between runs
            f"CREATE STAGE IF NOT EXISTS {STAGE}",
            f"""CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT}
                    TYPE = CSV
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    SKIP_HEADER = 1""",
        ])

    @task
    def get_new_files() -> list[str]:
        """Return only the files that are not yet in the control table."""
        rows = SnowflakeHook(snowflake_conn_id="snowflake").get_records(
            f"SELECT FILE_NAME FROM {CONTROL_TABLE}"
        )
        loaded = {row[0] for row in rows}
        new_files = [f for f in FILES if f not in loaded]
        print(f"Already loaded: {sorted(loaded)}")
        print(f"To load: {new_files}")
        return new_files

    @task
    def extract(file_name: str) -> str:
        """Upload one CSV from the local raw folder to the Snowflake stage."""
        src = DATA_DIR / file_name
        if not src.exists():
            raise FileNotFoundError(src)

        result = SnowflakeHook(snowflake_conn_id="snowflake").get_records(
            f"PUT 'file://{src}' @{STAGE} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        print(f"PUT {file_name}: {result}")
        return file_name  # only the file name goes through XCom

    @task
    def load(file_name: str) -> None:
        """Create the raw table (all STRING) and COPY the staged file into it."""
        table = f"{FQ_SCHEMA}.{Path(file_name).stem.upper()}"
        # all STRING: raw keeps the data as delivered, dbt staging models cast types
        columns = ", ".join(f"{col} STRING" for col in read_headers(DATA_DIR / file_name))

        conn = SnowflakeHook(snowflake_conn_id="snowflake").get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE OR REPLACE TABLE {table} ({columns})")

                # full file name, not a prefix: avoids matching similarly named files
                cur.execute(f"""
                    COPY INTO {table}
                    FROM @{STAGE}/{file_name}.gz
                    FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
                    ON_ERROR = 'ABORT_STATEMENT'
                """)
                col_names = [d[0].lower() for d in cur.description]
                for row in cur.fetchall():
                    r = dict(zip(col_names, row))
                    print(f"{r['file']}: {r['status']}, rows loaded: {r['rows_loaded']}")

                # only reached if COPY succeeded (ABORT_STATEMENT raises otherwise)
                cur.execute(
                    f"INSERT INTO {CONTROL_TABLE} (FILE_NAME, LOAD_TIMESTAMP) "
                    "VALUES (%s, CURRENT_TIMESTAMP())",
                    (file_name,),
                )
        finally:
            conn.close()

    @task.bash(trigger_rule="none_failed")
    def transform_prepare() -> str:
        return "cd /opt/airflow/olist_dbt && dbt deps"
    
    @task.bash
    def transform() -> str:
        return "cd /opt/airflow/olist_dbt && dbt build"

    new_files = get_new_files()
    setup() >> new_files

    staged = extract.expand(file_name=new_files)
    load.expand(file_name=staged) >> transform_prepare() >> transform()


olist_pipeline()
