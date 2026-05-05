#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -d node_modules ]; then
  echo "Installing deps for loan-core..."
  npm install
fi
npm run start:dev
