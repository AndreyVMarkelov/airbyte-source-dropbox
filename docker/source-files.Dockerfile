FROM python:3.12-slim

WORKDIR /airbyte/integration_code

COPY connectors/source-dropbox-files ./connectors/source-dropbox-files

RUN pip install --no-cache-dir ./connectors/source-dropbox-files

WORKDIR /airbyte/integration_code/connectors/source-dropbox-files

ENV AIRBYTE_ENTRYPOINT="source-dropbox-files"

ENTRYPOINT ["source-dropbox-files"]
