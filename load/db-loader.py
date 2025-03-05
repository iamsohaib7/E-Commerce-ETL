import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (UUID, DateTime, Float, Integer, String, Text,
                        create_engine)

BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_PATH = BASE_DIR / "Dataset/Raw-Dataset/E-Commerce/"
load_dotenv(BASE_DIR / ".env")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB", "Olist_Ecommerce")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# sqlalchemy variables
DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)

CREDENTIALS = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_PORT", "POSTGRES_HOST")
if not all(cred in os.environ for cred in CREDENTIALS):
    raise ValueError(f"Please set environment variables {CREDENTIALS}")


def main():

    # loading the dataset to db
    # customers table
    customers_df = pd.read_csv(DATASET_PATH / "olist_customers_dataset.csv")
    customers_df.to_sql(
        "customers",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "customer_id": UUID,
            "customer_unique_id": UUID,
            "customer_zip_code_prefix": Integer,
            "customer_city": String(50),
            "customer_state": String(5),
        },
    )

    # geolocations table
    geolocations_df = pd.read_csv(DATASET_PATH / "olist_geolocation_dataset.csv")
    geolocations_df.to_sql(
        "geolocations",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "geolocation_zip_code_prefix": Integer,
            "geolocation_lat": Float,
            "geolocation_lon": Float,
            "geolocation_city": String(50),
            "geolocation_state": String(5),
        },
        method="multi",
        chunksize=10000,
    )

    # order items table
    order_items_df = pd.read_csv(DATASET_PATH / "olist_order_items_dataset.csv")
    order_items_df.rename(columns={"order_item_id": "quantity"}, inplace=True)
    order_items_df["order_items_id"] = order_items_df.index
    order_items_df["total_price"] = (
        order_items_df["quantity"] * order_items_df["price"]
        + order_items_df["freight_value"]
    )
    order_items_df.to_sql(
        "order_items",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "order_items_id": Integer,
            "order_id": UUID,
            "quantity": Integer,
            "product_id": UUID,
            "seller_id": UUID,
            "shipping_limit_date": DateTime,
            "price": Float,
            "freight_value": Float,
            "total_price": Float,
        },
        method="multi",
        chunksize=10000,
    )

    # order payments table
    order_payments_df = pd.read_csv(DATASET_PATH / "olist_order_payments_dataset.csv")
    order_payments_df.to_sql(
        "order_payments",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "order_id": UUID,
            "payment_sequential": Integer,
            "payment_type": String(50),
            "payment_installments": Integer,
            "payment_value": Float,
        },
        method="multi",
        chunksize=10000,
    )

    # order reviews table
    order_reviews_df = pd.read_csv(DATASET_PATH / "olist_order_reviews_dataset.csv")
    order_reviews_df.to_sql(
        "order_reviews",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "review_id": UUID,
            "order_id": UUID,
            "review_comment_title": String(50),
            "review_comment_message": Text,
            "review_creation_date": DateTime,
            "review_answer_time": DateTime,
        },
        method="multi",
        chunksize=10000,
    )

    # orders table
    orders_df = pd.read_csv(DATASET_PATH / "olist_orders_dataset.csv")
    orders_df.to_sql(
        "orders",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "order_id": UUID,
            "customer_id": UUID,
            "order_status": String(50),
            "order_purchase_timestamp": DateTime,
            "order_approved_at": DateTime,
            "order_delivered_carrier_date": DateTime,
            "order_delivered_customer_date": DateTime,
            "order_estimated_delivery_date": DateTime,
        },
        method="multi",
        chunksize=10000,
    )

    # sellers table
    sellers_df = pd.read_csv(DATASET_PATH / "olist_sellers_dataset.csv")
    sellers_df.to_sql(
        "sellers",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "seller_id": UUID,
            "seller_zip_code_prefix": Integer,
            "seller_state": String(5),
            "seller_city": String(50),
        },
    )

    # products
    products_df = pd.read_csv(DATASET_PATH / "olist_products_dataset.csv")
    products_df.to_sql(
        "products",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "product_id": UUID,
            "product_category_name": String(100),
            "product_name_lenght": Integer,
            "product_description_lenght": Integer,
            "product_photos_qty": Integer,
            "product_weight_g": Integer,
            "product_length_cm": Integer,
            "product_height_cm": Integer,
            "product_width_cm": Integer,
        },
    )

    # product categories
    product_categories_df = pd.read_csv(
        DATASET_PATH / "product_category_name_translation.csv"
    )
    product_categories_df.to_sql(
        "product_categories",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "product_category_name": String(100),
            "product_category_name_english": String(100),
        },
    )


if __name__ == "__main__":
    main()
