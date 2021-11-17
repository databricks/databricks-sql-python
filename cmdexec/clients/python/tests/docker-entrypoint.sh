#!/bin/bash

set -e
export PYTHONPATH=$(pwd)

unit() {
  python cmdexec/clients/python/tests/tests.py 
}

integration() {
  # This is just a temporary hack until we have done the full migration and can change the import
  # names across the board
  touch qa/__init__.py
  touch qa/test/__init__.py
  touch qa/test/bi/__init__.py
  python test_executor.py "$@"
}

case "$1" in
unit)
    shift
    unit
    ;;
integration)
    shift
    integration "$@"
    ;;
*)
  echo "unrecognised command $1"
  exit 1
  ;;
esac

