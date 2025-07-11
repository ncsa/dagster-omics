"""
NeMO manifest asset for processing NeMO file manifests.
"""

import os
import tempfile
import shutil
from pathlib import Path

import boto3
import botocore.config
from boto3.s3.transfer import TransferConfig
from dagster import asset, Config
from dagster_ncsa import S3ResourceNCSA
import hashlib
import tarfile
import gzip

import requests

"""
ops:
  nemo_manifest:
    config:
      files: [
        {
          file_id: "SQ_BTR3002-01-5_S6_L001.fastq.tar",
          md5: "9a1b59b75ab5d7fffbd006b5e5087b2e",
          sample_id: "SQ_BTR3002-01-5",
          path_prefix: "biccn/grant/u19_zeng/zeng/transcriptome/bulk/10xMultiome_RNAseq/mouse/raw",
          size: 2371225600,
          url: "https://data.nemoarchive.org/biccn/grant/u19_zeng/zeng/transcriptome/bulk/10xMultiome_RNAseq/mouse/raw/SQ_BTR3002-01-5_S6_L001.fastq.tar"  # noqa: E501
        }
      ]
"""


class NeMOFile(Config):
    """Configuration for a single NeMO file entry."""
    file_id: str
    md5: str
    size: int
    url: str
    sample_id: str
    path_prefix: str


@asset(
    name="download_nemo_manifest",
    description="Download NeMO manifest files",
    group_name="nemo",
)
def download_nemo_manifest(context, s3: S3ResourceNCSA, config: NeMOFile):
    """
    Asset for downloading NeMO manifest files.
    """
    scratch_path = os.getenv("SCRATCH_PATH")
    download_dir = os.path.join(scratch_path, "nemo_manifest")
    os.makedirs(download_dir, exist_ok=True)

    context.log.info(f"Processing file {config.file_id} for sample {config.sample_id}")

    # Download the file
    size_gb = config.size / (1024 * 1024 * 1024)  # Convert bytes to GB
    context.log.info(f"Downloading {config.url} (size: {size_gb:.2f} GB)")
    output_path = Path(download_dir) / config.file_id
    download_file(config, output_path, context)

    # Extract tar files if applicable
    files_to_upload = []
    if config.file_id.endswith(".tar"):
        files_to_upload = untar_file(config.file_id, output_path, download_dir, context)
    else:
        files_to_upload.append(config.file_id)

    context.log.info(f"Successfully downloaded {files_to_upload}")


@asset(
    name="nemo_manifest",
    description="Process NeMO manifest files",
    group_name="nemo",
)
def nemo_manifest(context, s3: S3ResourceNCSA, config: NeMOFile):
    """
    Asset for processing NeMO manifest files.

    Args:
        context: Dagster context
        s3: S3 resource for accessing NeMO data
        config: NeMOFile configuration
    """
    # Create a temporary directory under SCRATCH_PATH
    scratch_path = os.getenv("SCRATCH_PATH")
    if not scratch_path:
        raise ValueError("SCRATCH_PATH environment variable is not set")

    temp_dir = tempfile.mkdtemp(dir=scratch_path)
    try:
        # Process the manifest files
        context.log.info(f"Processing file {config.file_id} for sample {config.sample_id}")

        # Download the file
        size_gb = config.size / (1024 * 1024 * 1024)  # Convert bytes to GB
        context.log.info(f"Downloading {config.url} (size: {size_gb:.2f} GB)")
        output_path = Path(temp_dir) / config.file_id
        download_file(config, output_path, context)

        # Extract tar files if applicable
        files_to_upload = []
        if config.file_id.endswith(".tar"):
            files_to_upload = untar_file(config.file_id, output_path, temp_dir, context)
        else:
            files_to_upload.append(config.file_id)

        bucket = os.getenv("DEST_BUCKET")
        s3_client = resilient_s3_client(s3)

        for file_to_upload in files_to_upload:
            file_path = Path(temp_dir) / file_to_upload

            context.log.info(f"Uploading {file_to_upload} to {config.path_prefix}")
            key = f"{config.path_prefix}/{file_to_upload}"
            upload_with_retry(s3_client, file_path, bucket, key, resilient_s3_transfer_config())

            # Clean up files after a successful upload
            if file_path.exists():
                file_path.unlink()
                context.log.info(f"Deleted decompressed file: {file_path}")

        return config
    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)
        context.log.info(f"Cleaned up temporary directory: {temp_dir}")


class UploadConfig(Config):
    src: str = "/var/dagster/scratch/nemo_manifest/P65M_1_RNA/P65M_1_RNA.bam"
    path_prefix: str = "biccn/grant/u19_huang/dulac/transcriptome/sncell/10x_multiome/mouse/processed/align"  # noqa: E501


@asset(
    name="upload_file",
    description="Upload NeMO manifest files",
    group_name="nemo",
)
def upload_file(context, s3: S3ResourceNCSA, config: UploadConfig):
    s3_client = resilient_s3_client(s3)
    bucket = os.getenv("DEST_BUCKET")
    file_to_upload = os.path.basename(config.src)
    key = f"{config.path_prefix}/{file_to_upload}"

    def human_readable_size(size):
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"

    size_bytes = Path(config.src).stat().st_size

    context.log.info(f"Uploading {config.src} ({human_readable_size(size_bytes)} to {key}")

    transfer_config = resilient_s3_transfer_config()
    # Upload the file
    upload_with_retry(s3_client, config.src, bucket, key, transfer_config)


def resilient_s3_client(s3: S3ResourceNCSA):
    """
    Create a resilient S3 client that retries on errors.
    """
    # Configure boto3 client
    s3_config = botocore.config.Config(
        retries={
            'max_attempts': 10,
            'mode': 'adaptive'
        },
        read_timeout=600,  # 10 minutes
        connect_timeout=120,  # 2 minutes

    )

    s3_client = boto3.client('s3',
                             aws_access_key_id=s3.aws_access_key_id,
                             aws_secret_access_key=s3.aws_secret_access_key,
                             endpoint_url=s3.endpoint_url,
                             config=s3_config)
    return s3_client


def resilient_s3_transfer_config():
    """
    Create a resilient S3 transfer configuration with optimized settings.

    Returns:
        TransferConfig: Configured S3 transfer settings with:
            - 25MB multipart threshold
            - 10 concurrent transfers
            - 25MB chunk size
            - Multi-threading enabled
    """
    transfer_config = TransferConfig(
        multipart_threshold=1024 * 25,  # 25MB
        max_concurrency=10,
        multipart_chunksize=1024 * 100,  # Increase to 100MB
        use_threads=True
    )
    return transfer_config


def upload_with_retry(s3_client, file_path, bucket, key, config, max_retries=3):
    for attempt in range(max_retries):
        try:
            s3_client.upload_file(file_path, bucket, key, Config=config)
            return
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'InvalidPart' and attempt < max_retries - 1:
                print(f"Upload attempt {attempt + 1} failed, retrying...")
                continue
            raise


def download_file(config: NeMOFile, output_path: Path, context) -> str:
    max_retries = 3
    timeout = (120, 3600 * 2)  # (connect timeout, read timeout) in seconds
    chunk_size = 1024 * 1024  # 1MB chunks

    for attempt in range(max_retries):
        try:
            # Download with streaming enabled
            with requests.get(config.url, stream=True, timeout=timeout) as response:
                response.raise_for_status()  # Raise an exception for bad status codes
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                last_percentage = 0

                # Initialize MD5 hash
                md5_hash = hashlib.md5()

                # Download the file in chunks and calculate MD5 simultaneously
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            md5_hash.update(chunk)
                            downloaded += len(chunk)
                            # Log progress at 10% intervals
                            percentage = int((downloaded / total_size) * 100)
                            if percentage >= last_percentage + 10:
                                context.log.info(f"Download progress: {percentage}%")
                                last_percentage = percentage

            context.log.info(f"Successfully downloaded {config.file_id}")

            # Get the final MD5 hash
            file_md5 = md5_hash.hexdigest()
            context.log.info(f"Verifying MD5 checksum for {config.file_id}")

            if file_md5 == config.md5:
                context.log.info(f"MD5 checksum verified: {file_md5}")
                return output_path
            else:
                raise ValueError(
                    f"MD5 checksum mismatch for {config.file_id}. Expected: {config.md5}, Got: {file_md5}"  # noqa: E501
                )

        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            if attempt < max_retries - 1:
                context.log.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
                context.log.info(f"Retrying download... (attempt {attempt + 2} of {max_retries})")
                # Clean up the partial file before retrying
                if output_path.exists():
                    output_path.unlink()
                continue
            else:
                context.log.error(f"All download attempts failed for {config.file_id}")
                raise

    raise Exception(f"Failed to download {config.file_id} after {max_retries} attempts")


def untar_file(file_id: str, file_path: Path, temp_dir: str, context) -> list[str]:
    """
    Untar a file and return the paths to the untarred files.
    Args:
        file_id (str): ID of the file being processed
        file_path (Path): Path to the tar file to extract
        temp_dir (str): Directory to extract files into
        context: Dagster context for logging

    Returns:
        list[str]: List of filenames that were extracted
    """
    context.log.info(f"Extracting tar file {file_id}")
    with tarfile.open(file_path, "r") as tar:
        # Extract all files to temp directory
        tar.extractall(path=temp_dir)
        # Get list of extracted files
        files_to_upload = [member.name for member in tar.getmembers() if member.isfile()]
        context.log.info(f"Extracted {len(files_to_upload)} files from {file_id}")

        for file_name in files_to_upload:
            context.log.info(f"  - {file_name}")

        return files_to_upload


def decompress_gz_file(file_path: Path, temp_dir: str, context) -> str:
    """
    Decompress a .gz file and return the path to the decompressed file.

    Args:
        file_path: Path to the .gz file
        temp_dir: Temporary directory path
        context: Dagster context for logging

    Returns:
        str: Path to the decompressed file
    """
    file_name = file_path.name
    decompressed_name = file_name[:-3]  # Remove .gz extension
    decompressed_path = Path(temp_dir) / decompressed_name
    context.log.info(f"Decompressing {file_name}")

    with gzip.open(file_path, "rb") as f_in:
        with open(decompressed_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Delete the compressed file
    file_path.unlink()
    return str(decompressed_path)
