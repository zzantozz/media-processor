#!/bin/bash

set -e

debug() {
  [ "$DEBUG" = true ] && echo "$1"
}
export -f debug

now="$(date +%s)"
debug "time: $now"

. db.sh
. file_analyzers.sh

# For all existing files, check if they're newly created or updated.
AMAZON_DIR=/mnt/d/amazon-drive/Amazon\ Drive/
find "$AMAZON_DIR" -type f -print0 | xargs -0 -I {} bash -c "analyze_existing_actual_file '{}' $now"

# For all files that should exist according to the db, check if they still exist.
while read -r line; do
  analyze_existing_db_file "$line" "$now"
done < <(sqlite3 test.db "select distinct file_path from file_events where file_path not in (select file_path from file_events where event_type = 'delete');")
