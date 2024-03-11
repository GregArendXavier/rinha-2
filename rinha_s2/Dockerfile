FROM ubuntu:22.04 as builder

RUN apt-get update && apt-get -y --no-install-recommends install \
    build-essential \
    libpq-dev \
    libjansson-dev \