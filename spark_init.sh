#!/bin/bash
pip install numpy
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@"
