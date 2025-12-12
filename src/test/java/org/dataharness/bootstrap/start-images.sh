#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$DIR" && cd ../../../../../.. && pwd)"

echo "Cleaning up existing containers..."
cd "$DIR"
docker compose down -v 2>/dev/null || true

echo "Building data-harness image with Maven Jib..."
cd "$PROJECT_ROOT"
mvn clean compile jib:dockerBuild

if [ $? -ne 0 ]; then
  echo "Failed to build Docker image with Maven Jib"
  exit 1
fi

cd "$DIR"
docker compose "$@"
