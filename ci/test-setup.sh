#!/usr/bin/env sh
if test -f "build/data/dump.not-compressed"; then
  echo "Test data already exists"
else
  pgbench -i -h postgres -U postgres postgres
  psql -h postgres -U postgres -d postgres -q -o /dev/null -f fixtures/schema.sql
  bin/generate-fixture-data.py -U postgres -h postgres -p 5432 -d postgres
  pg_dump -Fc -h postgres -U postgres -f build/data/dump.not-compressed -d postgres --compress=0
  pg_dump -Fc -h postgres -U postgres -f build/data/dump.compressed -d postgres --compress=9
  pg_dump -Fc -h postgres -U postgres -f build/data/dump.no-data -d postgres --compress=0 -s
  pg_dump -Fc -h postgres -U postgres -f build/data/dump.data-only -d postgres --compress=0 -a
  pg_dump -Fc -h postgres -U postgres -f build/data/dump.inserts -d postgres --compress=0 --inserts
fi
