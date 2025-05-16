FROM python:3.12-slim
WORKDIR /project
COPY pyproject.toml /project/

# Install uv
RUN pip install uv

# Use uv for package installation
RUN uv pip install --system .
COPY src/dagster_omics/ /project/dagster_omics/
RUN uv pip install --system -e .
