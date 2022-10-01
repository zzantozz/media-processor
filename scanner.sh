fun() {
: ${DEBUG:=false}

sha1="$(sha1sum "$1" | cut -d ' ' -f 1)" 
now="$(date +%s)"
path="${1//\'/\'\'}"
cur_stmt="select * from file_events where file_path = '$path';"
cur="$(sqlite test.db "$cur_stmt")"
IFS='|' read -r -a cur_fields <<< "$cur"
if [[ "${cur_fields[0]}" = created ]]; then
  echo "setting stmt1"
  stmt="foobie!"
else
  echo "setting stmt2"
  stmt="insert into file_events(type, file_path, time, storage_location, sha1) values('created', '$path', $now, 'Amazon', '$sha1');"
fi
if [[ "$DEBUG" = true ]]; then
  echo " -- $1"
  echo "path: $path"
  echo "sha1: $sha1"
  echo "updated: $now"
  echo "current state: ${cur_fields[*]}"
  echo "stmt: $stmt"
fi
attempts=0
while ! sqlite test.db "$stmt" &> /dev/null; do
  attempts=$((attempts+1))
  if [ $attempts -ge 2 ]; then
    echo "Insert attempt $attempts failed, retrying..."
    sleep 1
  fi
done
}
