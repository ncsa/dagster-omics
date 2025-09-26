# Dagster Omics

A Dagster-based data processing pipeline for downloading and analyzing genomic data from sources like the NCI Genomic Data Commons and NeMO Archive.

## Overview

This project provides automated workflows for:
- Downloading genomic data files from remote archives
- Processing and validating data integrity (MD5 checksums)
- Uploading processed data to S3-compatible storage
- Orchestrating complex bioinformatics workflows

## Architecture

### Main Components

The project consists of two main components:

#### 1. Core Dagster Pipeline (`src/dagster_omics/`)
- **Assets**: Core data processing assets for NeMO manifest handling
- **Sensor**: Automated monitoring and triggering of data processing jobs
- **Resources**: S3 integration for data storage and retrieval

#### 2. Cell Ranger Application (`applications/cell-ranger/`)
A separate sub-project for Cell Ranger analysis workflows, including:
- Docker containerization for reproducible environments
- Jupyter notebook for interactive analysis (`Omics.ipynb`)
- Dask-based distributed computing setup

### Key Features

- **NeMO Archive Integration**: Download and process files from the NeMO (NCI Genomic Data Commons) archive
- **Automated Data Pipeline**: Sensor-driven processing of manifest files
- **Data Integrity**: MD5 checksum validation for all downloaded files
- **S3 Storage**: Resilient upload to S3-compatible storage with retry logic
- **Scalable Processing**: Support for large genomic datasets with progress tracking

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd dagster-omics
```

2. Install dependencies using uv:
```bash
uv sync
```

3. For development with additional tools:
```bash
uv sync --extra dev
```

## Configuration

### Environment Variables

Set the following environment variables:

```bash
export AWS_S3_ENDPOINT_URL=<your-s3-endpoint>
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export DEST_BUCKET=<destination-bucket>
export MANIFEST_PREFIX=<manifest-file-prefix>
export SCRATCH_PATH=<temporary-processing-directory>
```

## Usage

### Running the Dagster Pipeline

1. Start the Dagster web server:
```bash
uv run dagster dev
```

2. Access the web interface at `http://localhost:3000`

### Assets and Sensors

The pipeline includes:

- **`nemo_manifest`** asset: Downloads and processes NeMO manifest files
- **`download_nemo_manifest`** asset: Handles file downloads with validation
- **`upload_file`** asset: Uploads processed files to S3
- **`nemo_manifest_sensor`**: Automatically detects new manifest files and triggers processing

### Cell Ranger Sub-Project

Navigate to the `applications/cell-ranger/` directory for Cell Ranger-specific workflows:

```bash
cd applications/cell-ranger/
uv pip install -r requirements.txt
uv run jupyter notebook Omics.ipynb
```

## Development

### Code Style
- Black formatting (line length: 100)
- isort for import sorting
- flake8 for linting
- mypy for type checking

### Testing
```bash
uv run pytest tests/
```

Note: Integration tests are excluded from default test runs.

## License

See [LICENSE](LICENSE) for details.

## Project Structure

```
dagster-omics/
├── src/dagster_omics/           # Main Dagster application
│   ├── assets/                  # Data processing assets
│   │   └── nemo_manifest.py     # NeMO file processing
│   ├── sensor.py                # Automated job triggering
│   └── definitions.py           # Dagster definitions
├── applications/
│   └── cell-ranger/             # Cell Ranger sub-project
│       ├── Dockerfile           # Container configuration
│       ├── Omics.ipynb         # Analysis notebook
│       └── requirements.txt     # Python dependencies
├── tests/                       # Test suite
└── workspace.yaml              # Dagster workspace configuration
```