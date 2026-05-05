#!/usr/bin/env bash
set -euo pipefail
cd services/loan-core
if [ ! -d node_modules ]; then
  echo "Installing deps for loan-core..."
  npm install
fi
npm run start:dev
