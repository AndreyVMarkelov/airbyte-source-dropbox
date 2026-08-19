FROM python:3.12-slim

WORKDIR /airbyte/integration_code

COPY pyproject.toml README.md LICENSE ./
COPY source_dropbox ./source_dropbox
COPY destination_dropbox ./destination_dropbox

RUN pip install --no-cache-dir .

ENTRYPOINT ["destination-dropbox"]
