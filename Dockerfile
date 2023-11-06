FROM 839591177169.dkr.ecr.us-west-2.amazonaws.com/open-source-mirror/dockerhub/python:3.11-slim
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
#install crdb 22.2
RUN wget --quiet --no-clobber "--output-document=cockroach-v22.2.9.linux-amd64.tgz" "https://binaries.cockroachdb.com/cockroach-v22.2.9.linux-amd64.tgz"
RUN echo "7ed169bf5f1f27bd49ab4e04a00068f7b44cff8a0672778b0f67d87ece3de07b  cockroach-v22.2.9.linux-amd64.tgz" | sha256sum -c
RUN tar xvf "cockroach-v22.2.9.linux-amd64.tgz"
RUN install "cockroach-v22.2.9.linux-amd64/cockroach" "/usr/local/bin/crdb22"
RUN rm -rf cockroach-v22.2.9.linux-amd64*
#install crdb 23.1
RUN wget --quiet --no-clobber "--output-document=cockroach-v23.1.2.linux-amd64.tgz" "https://binaries.cockroachdb.com/cockroach-v23.1.2.linux-amd64.tgz"
RUN echo "6924da4e047f8d6bba4cb6c7d844a5377002c9a60074e6bc8813c780f9d2d71c  cockroach-v23.1.2.linux-amd64.tgz" | sha256sum -c
RUN tar xvf "cockroach-v23.1.2.linux-amd64.tgz"
RUN install "cockroach-v23.1.2.linux-amd64/cockroach" "/usr/local/bin/crdb23"
RUN rm -rf cockroach-v23.1.2.linux-amd64*

ENV PROD_IAM_ROLE=arn:aws:iam::611706558220:role/storage-workflows \
    STAGING_IAM_ROLE=arn:aws:iam::914801092467:role/storage-workflows \
    CRDB_CLIENT=root \
    CRDB_PROD_HOST_SUFFIX=-crdb.us-west-2.aws.ddnw.net \
    CRDB_STAGING_HOST_SUFFIX=-crdb.us-west-2.aws.ddnw.net.staging.crdb.ddnw.net \
    CRDB_PORT=26257 \
    CRDB_CONNECTION_SSL_MODE=require \
    CRDB_CERTS_DIR_PATH_PREFIX=/app/crdb/certs \
    CRDB_CA_CERT_FILE_NAME=ca.crt \
    CRDB_PUBLIC_CERT_FILE_NAME=client.root.crt \
    CRDB_PRIVATE_KEY_FILE_NAME=client.root.key \
    CHRONOSPHERE_API_TOKEN="cd1bf1bf8bfb8fa8a932ee136f11f78a121369f84a9ee5acff3542abc09bd1c2" \
    CHRONOSPHERE_URL="doordash.chronosphere.io" \
    CHRONOSPHERE_PROMETHEUS_URL="https://doordash.chronosphere.io/data/metrics/api/v1/query" \
    SLACK_WEBHOOK_STORAGE_ALERT_TEST="https://hooks.slack.com/services/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" \
    SLACK_WEBHOOK_STORAGE_ALERTS_CRDB="https://hooks.slack.com/services/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" \
    SLACK_WEBHOOK_STORAGE_ALERTS_CRDB_STAGING="https://hooks.slack.com/services/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" \
    SLACK_WEBHOOK_STORAGE_OPERATIONS_LOG="https://hooks.slack.com/services/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"
