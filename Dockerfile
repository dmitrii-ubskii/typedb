ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
