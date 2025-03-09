import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv
import os
import logging
logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")


def create_tables_in_snowflake(query: str):
    """Use this function to create tables in Snowflake, before creating set the environment variables."""
    with snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        database=SNOWFLAKE_DATABASE,
    ) as ctx:
        with ctx.cursor() as cursor:
            try:
                cursor.execute(query)
                return True
            except snowflake.connector.errors.ProgrammingError as e:
                logging.error(e)
        return False
