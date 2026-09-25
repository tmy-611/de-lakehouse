#!/bin/bash
# docker/postgres/init/00-init-databases.sh
set -euo pipefail
for db in olist_source airflow warehouse; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
done       