from dagster_ncsa import S3ResourceNCSA
from dotenv import load_dotenv
import os

load_dotenv()


def test_nemo_manifest_sensor():
    bucket = os.getenv("DEST_BUCKET")
    prefix = os.getenv("MANIFEST_PREFIX")
    s3 = S3ResourceNCSA(
        endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    files = s3.list_files(bucket, prefix, extension=".tsv")
    print(files)
    assert len(files) > 0
