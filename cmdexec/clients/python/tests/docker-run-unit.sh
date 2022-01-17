#!/bin/bash

set -e

docker run $(./cmdexec/clients/pysql_v2-test-container-full_binary_loader) unit
