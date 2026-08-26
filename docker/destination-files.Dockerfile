FROM python:3.12-slim

WORKDIR /airbyte/integration_code
COPY connectors/destination-dropbox-files ./connectors/destination-dropbox-files
RUN pip install --no-cache-dir ./connectors/destination-dropbox-files
WORKDIR /airbyte/integration_code/connectors/destination-dropbox-files

ENV AIRBYTE_ENTRYPOINT="destination-dropbox-files"

ENTRYPOINT ["destination-dropbox-files"]
