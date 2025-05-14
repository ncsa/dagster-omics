"""
NeMO manifest asset for processing NeMO file manifests.
"""
from typing import List
import os
import requests
import tempfile
import shutil
from pathlib import Path
from dagster import AssetIn, asset, Config
from dagster_ncsa import S3ResourceNCSA
import hashlib
import tarfile
import gzip

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
          url: "https://data.nemoarchive.org/biccn/grant/u19_zeng/zeng/transcriptome/bulk/10xMultiome_RNAseq/mouse/raw/SQ_BTR3002-01-5_S6_L001.fastq.tar"
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

class NeMOManifest(Config):
    """Configuration for a NeMO manifest containing multiple file entries."""
    files: List[NeMOFile]

@asset(
    name="nemo_manifest",
    description="Process NeMO manifest files",
    ins={},
    group_name="nemo",
)
def nemo_manifest(context, s3: S3ResourceNCSA, config: NeMOManifest):
    """
    Asset for processing NeMO manifest files.
    
    Args:
        context: Dagster context
        s3: S3 resource for accessing NeMO data
        config: NeMOManifest configuration containing file entries
    """
    # Create a temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        # Process the manifest files
        for file_entry in config.files:
            context.log.info(f"Processing file {file_entry.file_id} for sample {file_entry.sample_id}")
            
            # Download the file
            context.log.info(f"Downloading {file_entry.url}")
            # output_path = Path(temp_dir) / file_entry.file_id
            output_path = "/Users/bengal1/dev/ncsa/dagster-omics/SQ_BTR3002-01-5_S6_L001.fastq.tar"
            
            # Download with streaming enabled
            # with requests.get(file_entry.url, stream=True) as response:
            #     response.raise_for_status()  # Raise an exception for bad status codes
            #     total_size = int(response.headers.get('content-length', 0))
            #     downloaded = 0
            #     last_percentage = 0
                
            #     # Download the file in chunks
            #     with open(output_path, 'wb') as f:
            #         for chunk in response.iter_content(chunk_size=1024*1024): # 1MB chunks
            #             if chunk:
            #                 f.write(chunk)
            #                 downloaded += len(chunk)
            #                 # Log progress at 10% intervals
            #                 percentage = int((downloaded / total_size) * 100)
            #                 if percentage >= last_percentage + 10:
            #                     context.log.info(f"Download progress: {percentage}%")
            #                     last_percentage = percentage
            
            context.log.info(f"Successfully downloaded {file_entry.file_id}")
            
            # Verify MD5 checksum
            context.log.info(f"Verifying MD5 checksum for {file_entry.file_id}")
            with open(output_path, 'rb') as f:
                file_md5 = hashlib.md5(f.read()).hexdigest()
            
            if file_md5 == file_entry.md5:
                context.log.info(f"MD5 checksum verified: {file_md5}")
            else:
                raise ValueError(f"MD5 checksum mismatch for {file_entry.file_id}. Expected: {file_entry.md5}, Got: {file_md5}")            
                    
            # Extract tar files if applicable
            files_to_upload = []
            if file_entry.file_id.endswith('.tar'):
                context.log.info(f"Extracting tar file {file_entry.file_id}")
                with tarfile.open(output_path, 'r') as tar:
                    # Extract all files to temp directory
                    tar.extractall(path=temp_dir)
                    # Get list of extracted files
                    files_to_upload = [member.name for member in tar.getmembers() if member.isfile()]
                    context.log.info(f"Extracted {len(files_to_upload)} files from {file_entry.file_id}")
                    
                    # Decompress .gz files
                    updated_files = []
                    for file_name in files_to_upload:
                        file_path = Path(temp_dir) / file_name
                        if file_name.endswith('.gz'):
                            # Decompress the file
                            decompressed_name = file_name[:-3]  # Remove .gz extension
                            decompressed_path = Path(temp_dir) / decompressed_name
                            context.log.info(f"Decompressing {file_name}")
                            
                            with gzip.open(file_path, 'rb') as f_in:
                                with open(decompressed_path, 'wb') as f_out:
                                    shutil.copyfileobj(f_in, f_out)
                            
                            # Delete the compressed file
                            file_path.unlink()
                            updated_files.append(decompressed_name)
                            context.log.info(f"  - {decompressed_name} (decompressed)")
                        else:
                            updated_files.append(file_name)
                            context.log.info(f"  - {file_name}")
                    
                    files_to_upload = updated_files
            else:
                files_to_upload.append(file_entry.file_id)

            bucket = os.getenv("DEST_BUCKET")
            s3_client = s3.get_client()

            for file_to_upload in files_to_upload:
                context.log.info(f"Uploading {file_to_upload} to {file_entry.path_prefix}")
                key = f"{file_entry.path_prefix}/{file_to_upload}"
                s3_client.upload_file(Path(temp_dir) / file_to_upload, bucket, key)
            
                    
    return config 