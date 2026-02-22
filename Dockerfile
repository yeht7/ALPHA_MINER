FROM python:3.12-slim

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    wget \
    vim \
    htop \
    net-tools \
    iputils-ping \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_LINK_MODE=copy

WORKDIR /workspace

EXPOSE 8000

CMD ["bash"]
