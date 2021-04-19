#!/bin/bash

CURR_DIR=$(cd $(dirname ${0}) && pwd)

# check variable has a value
function check_variable_is_empty() {
  local VAR_NAME=${1}
  if [[ -z ${VAR_NAME} ]]; then
    echo "ERROR: unset environment variable at the first argument"
    exit 1
  fi

  if [[ -z ${!VAR_NAME} ]]; then
    echo "ERROR: unset environment variable: ${VAR_NAME}"
    exit 1
  fi
}

function check() {
  check_variable_is_empty "GOOGLE_APPLICATION_CREDENTIALS"
  check_variable_is_empty "VAULT_ADDR"
  check_variable_is_empty "VAULT_TOKEN"
}

function init() {
  source ${CURR_DIR}/mount.sh
  source ${CURR_DIR}/init_db.sh
}

function bootstrap() {
  python3 coordinator.py
}

check \
  && init \
  && bootstrap
