# Please do not upgrade beyond python3.10.7 currently as dbt-spark does not support
# 3.11py and images do not get made properly
FROM python:3.10.7-slim-bullseye as base

# N.B. The refs updated automagically every release via bumpversion
ARG dbt_postgres_ref=dbt-postgres@v1.8.0a1
# special case args

# System setup
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

# Set docker basics
WORKDIR /usr/app/dbt/

## ENTRYPOINT ["dbt"]


# dbt-postgres
##
FROM base as dbt-postgres
RUN python -m pip install dbt-postgres --no-cache-dir

