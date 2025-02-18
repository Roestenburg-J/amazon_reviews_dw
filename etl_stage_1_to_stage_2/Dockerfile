FROM python:3.9

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy ETL script
COPY . /app

# Install cron
RUN apt-get update && apt-get install -y cron

# Add cron job (runs every 5 minutes)
RUN echo "*/5 * * * * root /app/etl_stage1_to_stage2.sh" >> /etc/crontab

# Start cron service and keep the container running
CMD cron -f
