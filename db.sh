# If you query a sqlite db while inserting, it can cause locking issues. I think there's another way to solve for this,
# but retries seem like a decent idea anyway.
db_execute_until_success() {
  [ "$#" -eq 2 ] || {
    echo "This function requires two args."
    echo "- db_name: name of the db file to execute the statement in"
    echo "- stmt: sql statement to execute"
    exit 1
  }
  db_name="$1"
  debug "db_name: $db_name"
  stmt="$2"
  debug "stmt: $stmt"
  attempts=0
  while ! sqlite "$db_name" "$stmt" &>/dev/null; do
    attempts=$((attempts + 1))
    if [ $attempts -ge 2 ]; then
      echo "Executing db statement attempt $attempts failed, retrying..."
      sleep 1
    fi
  done
}

export -f db_execute_until_success
