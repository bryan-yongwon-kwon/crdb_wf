FROM python:3.11-slim
WORKDIR /app

RUN apt update && apt install -y \
    git \
    awscli \
    jq \
    curl \
    wget \
    vim 

RUN pip install poetry
RUN poetry config virtualenvs.create false

ADD . /app

RUN poetry install

ENV PROD_IAM_ROLE=arn:aws:iam::611706558220:role/storage-workflows
ENV STAGING_IAM_ROLE=arn:aws:iam::914801092467:role/storage-workflows
ENV CRDB_CLIENT=root
ENV CRDB_HOST_SUFFIX=-crdb.us-west-2.aws.ddnw.net
ENV CRDB_PORT=26257
ENV CRDB_CONNECTION_SSL_MODE=require
ENV CRDB_CERTS_DIR_PATH_PREFIX=/app/crdb/certs
ENV CRDB_CA_CERT_FILE_NAME=ca.crt
ENV CRDB_PUBLIC_CERT_FILE_NAME=client.root.crt
ENV CRDB_PRIVATE_KEY_FILE_NAME=client.root.key