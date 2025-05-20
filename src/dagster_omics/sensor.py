import csv
import os
import dagster as dg
from dagster_ncsa import S3ResourceNCSA
from urllib.parse import urlparse

from dagster_omics.assets.nemo_manifest import NeMOFile

# Define a job that materializes the nemo_manifest asset
nemo_manifest_job = dg.define_asset_job(
    name="nemo_manifest_job",
    selection="nemo_manifest",
    description="Job to process NeMO manifest files and upload data to S3",
)


@dg.sensor(
    job=nemo_manifest_job,
    minimum_interval_seconds=10,
)
def nemo_manifest_sensor(context: dg.SensorEvaluationContext, s3: S3ResourceNCSA):
    bucket = os.getenv("DEST_BUCKET")
    manifest_prefix = os.getenv("MANIFEST_PREFIX")
    context.log.info(f"Bucket: {bucket}, manifest prefix: {manifest_prefix}")

    # List TSV files in the bucket
    manifest_files = s3.list_files(bucket, manifest_prefix, extension=".tsv")
    context.log.info(f"Found {len(manifest_files)} manifest files: {manifest_files}")

    s3_client = s3.get_client()
    for manifest_file in manifest_files:
        context.log.info(f"Processing manifest file: {manifest_file}")

        # Download the manifest file to a temporary location
        local_path = f"/tmp/{os.path.basename(manifest_file)}"
        s3_client.download_file(Bucket=bucket, Key=manifest_file, Filename=local_path)

        try:

            # Read the TSV file
            processed_count = 0
            with open(local_path, "r") as tsv_file:
                reader = csv.DictReader(tsv_file, delimiter="\t")

                for row in reader:
                    # Skip rows with missing required fields
                    if not row.get("file_id") or not row.get("urls"):
                        continue

                    # Extract the URL
                    url = row.get("urls")
                    file_id = row.get("file_id")

                    # Handle size field (convert to int or use -1 for NA)
                    size = row.get("size", "-1")
                    try:
                        size = int(size)
                    except ValueError:
                        size = -1

                    run_config = dg.RunConfig(
                        {
                            "nemo_manifest": NeMOFile(
                                file_id=file_id,
                                md5=row.get("md5", "NA"),
                                size=size,
                                url=url,
                                sample_id=row.get("sample_id", ""),
                                path_prefix=parse_url_path_prefix(url),
                            )
                        }
                    )

                    # Generate a unique run key based on the file_id
                    run_key = f"nemo_manifest_{file_id}"

                    # Yield a run request with configuration for this single file
                    yield dg.RunRequest(run_key=run_key, 
                                        run_config=run_config,
                                        tags={"nemo_manifest": manifest_file,
                                              "file_id": file_id,
                                              "size": size,
                                              "url": url,
                                              "sample_id": row.get("sample_id", "")}
                                        )

                    processed_count += 1
                    context.log.info(f"Created run request for file {file_id}")

            context.log.info(f"Processed {processed_count} files from manifest {manifest_file}")
            if processed_count == 0:
                context.log.warning(f"No valid entries found in {manifest_file}")

        except Exception as e:
            context.log.error(f"Error processing manifest file {manifest_file}: {str(e)}")

        finally:
            # Clean up the temporary file
            if os.path.exists(local_path):
                os.remove(local_path)


def parse_url_path_prefix(url):
    """
    Parses a URL to extract the path prefix up to but not including the filename.

    Args:
        url (str): The URL to parse

    Returns:
        str: The path prefix without the filename

    Example:
        Input: "https://data.nemoarchive.org/biccn/grant/u19_huang/dulac/transcriptome/sncell/10x_multiome/mouse/processed/align/E16F_1_RNA.bam.tar"  # noqa: E501
        Output: "biccn/grant/u19_huang/dulac/transcriptome/sncell/10x_multiome/mouse/processed/align"  # noqa: E501
    """
    # Parse the URL into components
    parsed_url = urlparse(url)

    # Get the path component
    path = parsed_url.path

    # Find the last '/' which separates the directory path from the filename
    last_slash_pos = path.rfind("/")
    if last_slash_pos == -1:
        return ""  # No directory part

    # Return everything before the last '/' and remove the leading '/'
    return path[1:last_slash_pos]
