fun() {
: ${DEBUG:=false}

sha1="$(sha1sum "$1" | cut -d ' ' -f 1)" 
now="$(date +%s)"
# File paths containing a single quote (') mess up the insert. This would be a good reason to move to a language that
# supports prepared statements. We can also just escape them by replacing every ' with ''.
path="${1//\'/\'\'}"
# Does this file exist yet?
find_current_stmt="select * from file_events where file_path = '$path';"
current="$(sqlite test.db "$find_current_stmt")"
IFS='|' read -r -a current_fields <<< "$current"
if [ -n "$current" ]; then
  echo "file path already exists. what next?"
else
  echo "file path not tracked, adding create event"
  stmt="insert into file_events(event_type, file_path, time, storage_location, sha1) "
  stmt+="values('created', '$path', $now, 'Amazon', '$sha1');"
fi
if [[ "$DEBUG" = true ]]; then
  echo " -- $1"
  echo "path: $path"
  echo "sha1: $sha1"
  echo "updated: $now"
  echo "current state: ${current_fields[*]}"
  echo "stmt: $stmt"
fi
if [ -n "$stmt" ]; then
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
