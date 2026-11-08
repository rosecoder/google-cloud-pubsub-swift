#!/usr/bin/env bash
set -euo pipefail

SOURCES_ROOT="$(pwd)/Sources"

GOOGLEAPIS_COMMIT="${GOOGLEAPIS_COMMIT:-d57f2c114b2d1d3db7fa71a1333d72129f8fd1ae}"

if [ ! -d googleapis/.git ]; then
  git init -q googleapis
  git -C googleapis remote add origin https://github.com/googleapis/googleapis.git
fi
echo "Fetching googleapis@${GOOGLEAPIS_COMMIT}..."
git -C googleapis fetch -q --depth 1 origin "${GOOGLEAPIS_COMMIT}"
git -C googleapis checkout -q --force FETCH_HEAD

rm -rf ${SOURCES_ROOT}/*/gRPC_generated/*

cd googleapis/

echo "Generating gRPC code..."
protoc google/pubsub/v1/*.proto \
  --swift_opt=Visibility=Package \
  --swift_out=${SOURCES_ROOT}/GoogleCloudPubSub/gRPC_generated/ \
  --grpc-swift_opt=Client=true,Server=false \
  --grpc-swift_opt=Visibility=Package \
  --grpc-swift_out=${SOURCES_ROOT}/GoogleCloudPubSub/gRPC_generated/
