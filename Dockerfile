# Use an official base image
FROM apache/airflow:latest

# Environment variables can be set at build time
ENV VERSION=0.0.6

# Copy just the requirements.txt and install dependencies first
# This improves Docker caching, as installing dependencies is likely
# the most time-consuming operation
COPY requirements.txt /requirements.txt

# Switch back to the airflow user
USER airflow

# Upgrade pip and install required Python packages
RUN pip install --user --upgrade pip && \
    pip install --no-cache-dir --user -r /requirements.txt

# Switch to root to install packages
USER root

# Update, upgrade, and install packages in one RUN statement, then clean up
RUN apt update && \
    apt dist-upgrade -y && \
    apt install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        nano \
        curl \
        iputils-ping \
        jq && \
    apt autoremove -yqq --purge && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm /requirements.txt
