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