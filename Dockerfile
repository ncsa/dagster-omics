FROM python:3.12-slim
WORKDIR /project
COPY pyproject.toml /project/

RUN pip install .
COPY src/dagster_omics/ /project/dagster_omics/
RUN pip install -e .
