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
# Get the current version number
CURRENT_VERSION=$(awk -F "=" '/^ENV VERSION=/ {print $2}' Dockerfile)

# Split the version into major, minor, and patch
IFS='.' read -ra VERSION_PARTS <<< "$CURRENT_VERSION"
MAJOR="${VERSION_PARTS[0]}"
MINOR="${VERSION_PARTS[1]}"
PATCH="${VERSION_PARTS[2]}"

# Increment the patch version by 1
NEW_PATCH=$((PATCH + 1))

# Construct the new version string
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"

# Split the version into major, minor, and patch
IFS='.' read -ra VERSION_PARTS <<< "$CURRENT_VERSION"
MAJOR="${VERSION_PARTS[0]}"
MINOR="${VERSION_PARTS[1]}"
PATCH="${VERSION_PARTS[2]}"

# Increment the patch version by 1
NEW_PATCH=$((PATCH + 1))

# Construct the new version string
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"

echo "Current Version: $CURRENT_VERSION"
echo "New Version: $NEW_VERSION"

# Use multi-arch buildx to build the Docker image for multiple platforms
docker buildx use naughty_agnesi

# Build the Docker image for amd64/linux and arm64/linux in one go
docker buildx build --push --platform linux/amd64,linux/arm64 . -t $DOCKER_REGISTRY/$DOCKER_IMAGE:$NEW_VERSION -t $DOCKER_REGISTRY/$DOCKER_IMAGE:latest

# Create and push the multi-platform manifest
#docker manifest create $DOCKER_REGISTRY/$DOCKER_IMAGE:latest \
#  $DOCKER_REGISTRY/$DOCKER_IMAGE:$NEW_VERSION --amend

#docker manifest annotate $DOCKER_REGISTRY/$DOCKER_IMAGE:latest \
#  $DOCKER_REGISTRY/$DOCKER_IMAGE:$NEW_VERSION --os linux --arch amd64

#docker manifest annotate $DOCKER_REGISTRY/$DOCKER_IMAGE:latest \
#  $DOCKER_REGISTRY/$DOCKER_IMAGE:$NEW_VERSION --os linux --arch arm64

#docker manifest push $DOCKER_REGISTRY/$DOCKER_IMAGE:latest

# If everything went ok, update the version number in the Dockerfile
sed -i.bak "s/^ENV VERSION=.*/ENV VERSION=$NEW_VERSION/" Dockerfile
rm Dockerfile.bak
