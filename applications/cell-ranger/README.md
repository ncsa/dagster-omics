# Cell Ranger ARC Application

This application provides a Dask-enabled interface for running [Cell Ranger ARC](https://www.10xgenomics.com/support/software/cell-ranger-arc/latest) analysis on single-cell multiome data (ATAC + Gene Expression).

## Overview

Cell Ranger ARC is an advanced analytical suite designed for the Chromium Single Cell Multiome ATAC + Gene Expression sequencing. It provides in-depth analysis of gene expression and chromatin accessibility at a single cell level, uniquely linking these aspects for enhanced genomic understanding.

## Components

### Docker Image (`Dockerfile`)

The Docker image provides a containerized environment with:
- **Base**: Python 3.11-slim
- **Cell Ranger ARC**: Version 2.0.2 installed from 10x Genomics
- **Dask Dependencies**: Complete Dask ecosystem for distributed computing
- **Purpose**: Enables Cell Ranger ARC to run in Dask worker nodes on HTC clusters

The image downloads and installs Cell Ranger ARC 2.0.2, making the `cellranger-arc` command available in the container's PATH.

Once the docker image is built, it can be pushed to a container registry. To use this 
image in the HTC cluster you will use the `apptainer build` command to create a SIF file. This .SIF file must be readable
by HTC workers. It's path is specified in the `container_image` parameter of the `gateway.new_cluster()` call.
`

### Jupyter Notebook (`Omics.ipynb`)

The notebook provides a complete workflow for running Cell Ranger ARC analysis:

#### Key Features:
1. **Cluster Setup**: Connects to HTC Dask Gateway with configurable resources (12 CPUs, 64GB memory)
2. **Site Check**: Validates Cell Ranger installation and system compatibility
3. **Count Analysis**: Comprehensive single-cell counting with support for:
   - Gene expression libraries
   - Chromatin accessibility libraries
   - Custom library configurations via CSV
   - Multiple FASTQ input directories
   - Reference transcriptome specification

#### Usage Pattern:
```python
# Connect to cluster
cluster = gateway.new_cluster(
    image="ncsa/cell-ranger-arc:2.0.2",
    cpus=12,
    memory="64GB",
    container_image="/path/to/cell-ranger.sif"
)

# Submit analysis job
future = client.submit(run_cellranger_count,
    id="sample_id",
    libraries=[
        {"fastqs": "/path/to/rna/fastqs", "sample": "sample_name", "library_type": "Gene Expression"},
        {"fastqs": "/path/to/atac/fastqs", "sample": "sample_name", "library_type": "Chromatin Accessibility"}
    ],
    transcriptome="/path/to/reference",
    working_dir="/scratch/output"
)
```

#### Key Functions:
- `run_cellranger_sitecheck()`: Validates system requirements and installation
- `run_cellranger_count()`: Executes the full Cell Ranger ARC count pipeline
- `get_available_memory()`: Reports available system memory for resource planning

## Requirements

- Access to HTC Dask Gateway
- Cell Ranger ARC-compatible reference transcriptome
- Properly formatted FASTQ files for both RNA and ATAC libraries
- Sufficient computational resources (recommended: 12+ CPUs, 64+ GB RAM)

## Output

Cell Ranger ARC generates comprehensive analysis outputs including:
- Feature-barcode matrices
- Analysis results (clustering, dimensionality reduction)
- Quality control metrics
- Web summary reports
- BAM files (optional)