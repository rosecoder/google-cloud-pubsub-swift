#!/bin/bash
# usage: queue.sh <file>; each line: <build> <label> [--cpus=N] args...  Stops early if ~/STOP exists.
while read -r line; do
  [[ -z "$line" || "$line" == \#* ]] && continue
  if [[ -e ~/STOP ]]; then echo "STOP found, skipping: $line" >> ~/results/queue.log; continue; fi
  echo "$(date -u +%T) begin $line" >> ~/results/queue.log
  # shellcheck disable=SC2086
  ~/run.sh $line
  echo "$(date -u +%T) end $(tail -1 ~/results/logs/$(echo $line | awk '{print $1"-"$2}').log)" >> ~/results/queue.log
done < "$1"
echo "$(date -u +%T) queue $1 done" >> ~/results/queue.log
