#!/bin/bash
set -eux
ruff check
mypy vastdb/
