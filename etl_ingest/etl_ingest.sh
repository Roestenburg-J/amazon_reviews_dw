#!/bin/bash

if pgrep -f "python3 /app/stage_0.py" > /dev/null; then
  echo "Ingest script is already running."
  exit 1
fi

python3 /app/stage_0.py