# Use Python 3.11 slim as base image
FROM python:3.11-slim

# Avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and upgrade installed packages
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y curl \
    # Clean up to reduce image size
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app
RUN curl -o cellranger-arc-2.0.2.tar.gz "https://cf.10xgenomics.com/releases/cell-arc/cellranger-arc-2.0.2.tar.gz?Expires=1752310294&Key-Pair-Id=APKAI7S6A5RYOXBWRPDA&Signature=Ln~Qk90rPaPEEhlE0PcIKd0P3fruDfKbcNAfFOdUYEESJ6BdLTe2w4QLeHlMYGmNmMPAvN-rxrjNqYO6cpMnSkfAnOrNbj2sb7ktD7eU~0HZmi~lWucd2yGsNKvX8st8QWN2hMseebrO1~CG2JgZXJ4bumANEg6Ewhosgtw6tu1HtRvoingOaahl6P4IPtobsv8k7S5QZ41aV~5Sv5HjU1sB9dD6tCfiSw0ZKAm~9tLW69WeSYiPeIcS5oE5khfyKlMSgpv9jAg-QtqdeBZr1moFZZhqPA9yfGldnMvbUn5JJix4QMklVNuSHMr1hx6ROBqYf4wJQ91q0Njrq7bZcQ__" \
 && tar -zxvf cellranger-arc-2.0.2.tar.gz

# Add CellRanger binary path to PATH environment variable
ENV PATH="/app/cellranger-arc-2.0.2/bin:${PATH}"

WORKDIR /dask
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

# Set the default command
CMD ["bash"]