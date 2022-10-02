#!/bin/bash

set -e

echo "Searches the db for rows that violate some constraint, meaning that YOU CODED SOMETHING WRONG!"
echo ""

violations=0

res="$(sqlite3 test.db "select * from file_events where event_type not in ('create', 'update', 'delete');")"
[ -n "$res" ] && {
  echo -e " *** Here are the rows with an invalid 'event_type':\n$res"
  violations=$((violations + 1))
}

echo "Found $violations areas with violations"
[ $violations -eq 0 ] || exit 1
