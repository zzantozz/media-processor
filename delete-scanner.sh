#!/bin/bash

set -e

export AMAZON_DIR=/mnt/d/amazon-drive/Amazon\ Drive/

debug() {
  [ "$DEBUG" = true ] && echo "$1"
}
export -f debug

fun() {
  debug " -- $1"
  path="${1//\'/\'\'}"
  debug "db-safe path: $path"
  now="$(date +%s)"
  debug "time: $now"
  if [ -f "$1" ]; then
    debug "state: file still exists"
  else
    debug "state: file was deleted"
    find_current_stmt="select sha1 from file_events where file_path = '$path' order by time desc limit 1;"
    current="$(sqlite test.db "$find_current_stmt")"
    debug "last sha1: $current"
    stmt="insert into file_events(event_type, file_path, time, storage_location, sha1) "
    stmt+="values('delete', '$path', $now, 'Amazon', '$current');"
    debug "stmt: $stmt"
    attempts=0
    # When I query the db while this is running, it can cause locking problems. If the insert fails, sleep and retry.
    while ! sqlite test.db "$stmt" &>/dev/null; do
      attempts=$((attempts + 1))
      if [ $attempts -ge 2 ]; then
        echo "Insert attempt $attempts failed, retrying..."
        sleep 1
      fi
    done
  fi
}

while read -r line; do
  fun "$line"
  # Run for every path that's not marked deleted
done < <(sqlite test.db "select distinct file_path from file_events where file_path not in (select file_path from file_events where event_type = 'delete');")
