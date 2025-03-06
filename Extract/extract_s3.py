import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError


def extract_s3(bucket: str, key: str):
    s3_client = boto3.client("s3")
    buffer = io.BytesIO()
    try:
        s3_client.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        return pd.read_parquet(buffer, engine="pyarrow")
    except ClientError as e:
        logging.error(e)
    return None
