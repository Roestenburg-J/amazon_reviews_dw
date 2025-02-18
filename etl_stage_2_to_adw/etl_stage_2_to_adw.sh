#!/bin/bash
# python3 /app/etl.py

# echo "Running the shell script..."

if pgrep -f "python3 /app/stage_2_to_adw.py" > /dev/null; then
  echo "Ingest script is already running."
  exit 1
fi

# echo "Starting Python script..."

python3 /app/stage_2_to_adw.py
