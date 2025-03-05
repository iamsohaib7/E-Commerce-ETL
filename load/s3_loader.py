import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)


def upload_df_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """
    it uploads a dataframe to an S3 bucket in parquet format
    :param key: str
    :param df: pd.DataFrame
    :param bucket: str
    :return: True if file was uploaded, else False
    """
    logging.info(f"Uploading {key} to {bucket}")
    parquet_buffer = io.BytesIO()
    # convert uuid type to str because parquet doesnt supports it
    for col in df.columns:
        if col.endswith("_id") and df[col].dtype == "object":
            df[col] = df[col].astype(str)
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False, compression="snappy")
    try:
        s3_resource = boto3.resource("s3")
        s3_resource.Bucket(bucket).put_object(Body=parquet_buffer.getvalue(), Key=key, ACL="private")
        logging.info(f"Uploaded {key} to {bucket}")
        return True
    except ClientError as e:
        logging.error(e)
    return False
