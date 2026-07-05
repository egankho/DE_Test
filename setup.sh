#!/usr/bin/env bash
# Checks for the dependencies needed to run and test this project
# on WSL2 Ubuntu/a Ubuntu machine
# 
# execute script in codebase's root dir.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv"

echo "==> Updating apt package index"
sudo apt-get update -y

echo "==> Installing OpenJDK 17 (required by Spark's JVM) and Python venv tooling"
sudo apt-get install -y openjdk-17-jdk python3-venv python3-pip

echo "==> Creating Python virtual environment at ${VENV_DIR}"
python3 -m venv "${VENV_DIR}"

source "${VENV_DIR}/bin/activate"

echo "==> Upgrading pip and installing project requirements"
pip install --upgrade pip
pip install -r "${PROJECT_ROOT}/dependencies.txt"

echo "==> Installing project package in editable mode"
pip install -e "${PROJECT_ROOT}"

cat <<EOF

Setup complete. If errors occur, kindly verify the networking status of
WSL2 as well as the java path in the shell profile (~/.bashrc)

Next steps (see README.md for full detail):
  1. source .venv/bin/activate
  2. flake8 src tests                     # lint check
  3. pytest                               # unit + integration tests
  4. ./scripts/test_with_sample_data.sh   # test Python module with sample data
EOF
