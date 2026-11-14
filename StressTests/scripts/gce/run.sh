#!/bin/bash
# usage: run.sh <build: main|base> <label> [--cpus=N] harness-args...
set -u
BUILD=$1; LABEL=$2; shift 2
DOCKER_EXTRA=()
if [[ "${1:-}" == --cpus=* ]]; then DOCKER_EXTRA+=("$1"); shift; fi
PROJECT=$(curl -s -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/project/project-id)
mkdir -p ~/results/logs
LOG=~/results/logs/$BUILD-$LABEL.log
echo "start $(date -u +%FT%TZ) build=$BUILD label=$LABEL docker=${DOCKER_EXTRA[*]:-} args=$*" > "$LOG"
docker run --rm --name "stress-$BUILD-$LABEL" --network host "${DOCKER_EXTRA[@]}" \
  --ulimit nofile=65536:65536 -e STRESS_PROJECT_ID="$PROJECT" \
  -v ~/work/$BUILD:/src -v ~/results:/results swift:6.4-noble \
  /src/StressTests/.build/release/PubSubStress out=/results/$BUILD label="$LABEL" "$@" >> "$LOG" 2>&1
echo "exit $? end $(date -u +%FT%TZ)" >> "$LOG"
