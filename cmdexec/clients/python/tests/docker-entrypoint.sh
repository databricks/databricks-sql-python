#!/bin/bash

set -e
export PYTHONPATH=$(pwd)

unit() {
  python cmdexec/clients/python/tests/tests.py 
}

integration() {
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

