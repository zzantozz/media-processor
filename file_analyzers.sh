#!/bin/bash

analyze_existing_actual_file() {
  [ "$#" -eq 2 ] || {
    echo "analyze_existing_actual_file() requires two args."
    echo "- file_path: relative path of the file to analyze"
    echo "- time: the time at which this scan is running; format is seconds since epoch"
    exit 1
  }
  file_path="$1"
  debug " -- $file_path"
  time="$2"
  sha1="$(sha1sum "$1" | cut -d ' ' -f 1)"
  debug "calculated sha1: $sha1"
  # File paths containing a single quote (') mess up the insert. This would be a good reason to move to a language that
  # supports prepared statements. We can also just escape them by replacing every ' with ''.
  path="${1//\'/\'\'}"
  debug "db-safe path: $path"
  # Does this file exist yet?
  find_current_stmt="select event_type, sha1 from file_events where file_path = '$path' order by time desc limit 1;"
  current="$(sqlite test.db "$find_current_stmt")"
  IFS='|' read -r -a current_fields <<<"$current"
  debug "current state: ${current_fields[*]}"
  if [ -n "$current" ]; then
    debug "state: file path is tracked"
    event_type="${current_fields[0]}"
    debug "event_type: $event_type"
    last_sha1="${current_fields[1]}"
    debug "sha1: $last_sha1"
    if [ "$event_type" = "delete" ]; then
      debug "state: deleted file is re-created"
      stmt="insert into file_events(event_type, file_path, time, storage_location, sha1) "
      stmt+="values('create', '$path', $time, 'Amazon', '$sha1');"
    # Make sure not to mark a newly re-created file as an update if it was just marked a create! A new file is a new file,
    # even if it happens to be named the same as a previously deleted one.
    elif [ ! "$last_sha1" = "$sha1" ]; then
      debug "state: file has been updated"
      stmt="insert into file_events(event_type, file_path, time, storage_location, sha1) "
      stmt+="values('update', '$path', $time, 'Amazon', '$sha1');"
    fi
  else
    debug "state: file path not tracked, adding create event"
    stmt="insert into file_events(event_type, file_path, time, storage_location, sha1) "
    stmt+="values('create', '$path', $time, 'Amazon', '$sha1');"
  fi
  if [ -n "$stmt" ]; then
    db_execute_until_success "test.db" "$stmt"
  fi
}

analyze_existing_db_file() {
  [ "$#" -eq 2 ] || {
    echo "analyze_existing_db_file() requires two args."
    echo "- file_path: relative path of the file to analyze"
    echo "- time: the time at which this scan is running; format is seconds since epoch"
    exit 1
  }
  file_path="$1"
  debug " -- $file_path"
  time="$2"
  path="${1//\'/\'\'}"
  debug "db-safe path: $path"
  if [ -f "$1" ]; then
    debug "state: file still exists"
  else
    debug "state: file was deleted"
    find_current_stmt="select sha1 from file_events where file_path = '$path' order by time desc limit 1;"
    current="$(sqlite test.db "$find_current_stmt")"
    debug "last sha1: $current"
    stmt="insert into file_events(event_type, file_path, time, storage_location, sha1) "
    stmt+="values('delete', '$path', $time, 'Amazon', '$current');"
    db_execute_until_success "test.db" "$stmt"
  fi
}

export -f analyze_existing_actual_file
export -f analyze_existing_db_file
