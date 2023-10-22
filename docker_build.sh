#!/bin/bash
###
### Docker Image Build Script
###

# Exit immediately if a command exits with a non-zero status.
set -e

# Variables
DOCKER_REGISTRY=${1:-"djh00t"}
DOCKER_IMAGE=${2:-"gobbler-airflow"}

# Get the current version number
CURRENT_VERSION=$(awk -F "=" '/^VERSION=/ {print $2}' Dockerfile)

# Increment the version number
NEW_VERSION=$((CURRENT_VERSION + 1))

# Use multi-arch buildx to build the Docker image for multiple platforms
docker buildx use naughty_agnesi

# Build the Docker image for amd64/linux and arm64/linux in one go
docker buildx build --platform linux/amd64,linux/arm64 . -t $DOCKER_REGISTRY/$DOCKER_IMAGE:$NEW_VERSION

# Push the Docker images to the registry
docker push $DOCKER_REGISTRY/$DOCKER_IMAGE:$NEW_VERSION

# Tag the Docker image with the latest tag
docker image tag $DOCKER_REGISTRY/$DOCKER_IMAGE:$NEW_VERSION $DOCKER_REGISTRY/$DOCKER_IMAGE:latest

# Push the Docker image with the latest tag to the registry
docker push $DOCKER_REGISTRY/$DOCKER_IMAGE:latest

# If everything went ok, update the version number in the Dockerfile
sed -i.bak "s/^VERSION=.*/VERSION=$NEW_VERSION/" Dockerfile && rm Dockerfile.bak
