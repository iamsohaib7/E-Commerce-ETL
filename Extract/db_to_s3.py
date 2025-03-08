import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from Load.s3_loader import upload_df_to_s3

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB", "Olist_Ecommerce")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# sqlalchemy variables
DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)


def main():
    table_names = (
        "customers",
        "geolocations",
        "product_categories",
        "products",
        "orders",
        "order_items",
        "order_reviews",
        "sellers",
        "order_payments",
    )
    for table_name in table_names:
        df = extract_table(table_name)
        if not upload_df_to_s3(
            df, bucket=BUCKET_NAME, key=f"Data/{table_name}.parquet"
        ):
            raise Exception(f"Failed to upload {table_name}")


def extract_table(table_name: str):
    df = pd.read_sql_table(table_name, engine)
    return df


if __name__ == "__main__":
    main()
