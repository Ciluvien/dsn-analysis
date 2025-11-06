#!/usr/bin/env sh

if [ -z "$1" ]; then
    echo "Usage: $0 <match_pattern>"
    exit 1
fi

curl -X POST -g "http://localhost:9090/api/v1/admin/tsdb/delete_series?match[]=$1"
curl -X POST http://localhost:9090/api/v1/admin/tsdb/clean_tombstones
curl -X PUT http://localhost:9090/api/v1/admin/tsdb/clean_tombstones
