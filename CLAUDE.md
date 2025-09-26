# Dagster-Omics
This repo contains code to run various omic pipelines within Dagster. It also
contains applications for the ingested data. These applications are designed to be run
in a dask cluster. The idea is that once you ingest the data you can run a calculation
on the data from a Jupyter notebook, with all of the dependencies installed in a docker
or apptainer container.

## Assets
NemoManifest - this downloads a dataset from the NeMO archive based on a manifest file that 
is usually produced via the NeMO web interface.

The manifest file is saved into a monitored s3 bucket. A sensor watches that bucket for
changes and downloads data based on the new manifest file.

## Applications
These are applications that are designed to be run in a dask cluster.

### cell-ranger
This application runs [cell ranger ARC](https://www.10xgenomics.com/support/software/cell-ranger-arc/latest)
which is 
> Cell Ranger ARC is an advanced analytical suite designed for the Chromium 
> Single Cell Multiome ATAC + Gene Expression sequencing. It provides in-depth 
> analysis of gene expression and chromatin accessibility at a single cell level, 
> uniquely linking these aspects for enhanced genomic understanding.

By providing a dockerfile that can run this program, we can deploy this into a dask
worker and invoke the command line cell-ranger-arc program within a python function
scheduled into the cluster.



