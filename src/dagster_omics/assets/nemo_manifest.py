"""
NeMO manifest asset for processing NeMO file manifests.
"""

import os
import tempfile
import shutil
from pathlib import Path
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
    temp_dir = os.path.join(scratch_path, "nemo_manifest")

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
        s3_client = s3.get_client()

        for file_to_upload in files_to_upload:
            file_path = Path(temp_dir) / file_to_upload
            original_path = file_path  # Keep track of original path
            # Decompress .gz files before uploading
            if file_to_upload.endswith('.gz'):
                context.log.info(f"Decompressing {file_to_upload} before upload")
                decompressed_path = decompress_gz_file(file_path, temp_dir, context)
                file_to_upload = Path(decompressed_path).name
                file_path = Path(decompressed_path)

            context.log.info(f"Uploading {file_to_upload} to {config.path_prefix}")
            key = f"{config.path_prefix}/{file_to_upload}"
            s3_client.upload_file(file_path, bucket, key)

            # Clean up files after successful upload
            if file_path.exists():
                file_path.unlink()
                context.log.info(f"Deleted decompressed file: {file_path}")
            if original_path.exists():
                original_path.unlink()
                context.log.info(f"Deleted original file: {original_path}")

        return config
    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)
        context.log.info(f"Cleaned up temporary directory: {temp_dir}")


def download_file(config: NeMOFile, output_path: Path, context) -> str:
    max_retries = 3
    timeout = (30, 3600 * 2)  # (connect timeout, read timeout) in seconds
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
