#!/bin/bash

set -e

docker run $(./cmdexec/clients/cmdexec-python-test-container-full_binary_loader) unit
