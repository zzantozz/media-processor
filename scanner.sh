#!/bin/bash

: "${DEBUG:=false}"

debug() {
  [ "$DEBUG" = true ] && echo "$1"
}
export -f debug

fun() {
debug " -- $1"
sha1="$(sha1sum "$1" | cut -d ' ' -f 1)"
debug "calculated sha1: $sha1"
now="$(date +%s)"
debug "time: $now"
# File paths containing a single quote (') mess up the insert. This would be a good reason to move to a language that
# supports prepared statements. We can also just escape them by replacing every ' with ''.
path="${1//\'/\'\'}"
debug "db-safe path: $path"
# Does this file exist yet?
find_current_stmt="select event_type, sha1 from file_events where file_path = '$path' order by time desc limit 1;"
current="$(sqlite test.db "$find_current_stmt")"
IFS='|' read -r -a current_fields <<< "$current"
debug "current state: ${current_fields[*]}"
if [ -n "$current" ]; then
  debug "file path already exists. what next?"
  debug "event_type: ${current_fields[0]}"
  debug "sha1: ${current_fields[1]}"
else
  debug "file path not tracked, adding create event"
  stmt="insert into file_events(event_type, file_path, time, storage_location, sha1) "
  stmt+="values('created', '$path', $now, 'Amazon', '$sha1');"
fi
if [ -n "$stmt" ]; then
  debug "stmt: $stmt"
  attempts=0
  # When I query the db while this is running, it can cause locking problems. If the insert fails, sleep and retry.
  while ! sqlite test.db "$stmt" &> /dev/null; do
    attempts=$((attempts+1))
    if [ $attempts -ge 2 ]; then
      echo "Insert attempt $attempts failed, retrying..."
      sleep 1
    fi
  done
fi
}
export -f fun
export AMAZON_DIR=/mnt/d/amazon-drive/Amazon\ Drive/
find "$AMAZON_DIR" -type f -print0 | xargs -0 -I {} bash -c 'fun "{}"'
