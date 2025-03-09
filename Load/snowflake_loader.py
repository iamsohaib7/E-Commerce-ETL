import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
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

def load_df_to_snowflake(table: str, df: pd.DataFrame):
    with snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
    ) as ctx:
        try:
            for column in df.columns:
                if column.endswith("_id") and df[column].dtype == "object":
                    df[column] = df[column].astype(str)
            cols = df.select_dtypes(include=["datetime64[ns]"]).columns
            for col in cols:
                df[col] = df[col].dt.tz_localize("utc")
            write_pandas(df=df, table_name=table, conn=ctx, use_logical_type=True)
            return True
        except snowflake.connector.errors.ProgrammingError as e:
            logging.error(e)
        return False