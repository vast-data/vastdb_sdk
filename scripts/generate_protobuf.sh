#!/bin/bash
set -eux

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="${ROOT_DIR}/resources/protobuf"
OUT_DIR="${ROOT_DIR}/vastdb/vast_protobuf/tabular"
PROTO_NAME="pysdk_blob_expansion"

protoc --version
protoc \
  -I "${PROTO_DIR}" \
  --python_out="${OUT_DIR}" \
  "${PROTO_DIR}/${PROTO_NAME}.proto"

echo "Generated ${OUT_DIR}/${PROTO_NAME}_pb2.py"
