"""
Core definitions for the dagster-omics application.
"""
from dagster import Definitions, EnvVar
from dagster_ncsa import S3ResourceNCSA

from dagster_omics.assets.nemo_manifest import nemo_manifest, download_nemo_manifest, \
    upload_file
from dagster_omics.sensor import nemo_manifest_sensor, nemo_manifest_job

# Import assets and jobs here as they are created
defs = Definitions(
    sensors=[nemo_manifest_sensor],
    assets=[nemo_manifest, download_nemo_manifest, upload_file],
    jobs=[nemo_manifest_job],
    resources={
        "s3": S3ResourceNCSA(
            endpoint_url=EnvVar("AWS_S3_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        ),
    }
)
