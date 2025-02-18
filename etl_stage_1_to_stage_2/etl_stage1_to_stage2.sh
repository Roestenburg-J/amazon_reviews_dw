# #!/bin/bash
# python3 /app/stage.py


if pgrep -f "python3 /app/stage_1_to_stage_2.py" > /dev/null; then
  echo "Ingest script is already running."
  exit 1
fi

python3 /app/stage_1_to_stage_2.py