#!/usr/bin/env bash
BASE_PATH=$(realpath "${0%/*}")
DDL_PATH=${BASE_PATH}/queries/ddl
DML_PATH=${BASE_PATH}/queries/dml
DB_NAME="metadata.db"

function import() {
    local SQL_PATH=${1}
    for entry in ${SQL_PATH}/*
    do
        sqlite3 ${DB_NAME} < ${entry}
    done
}

import ${DDL_PATH}
import ${DML_PATH}
