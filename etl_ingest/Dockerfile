FROM python:3.9

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install wait-for-it
RUN apt-get update && apt-get install -y curl
# RUN curl -sS https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -o /app/wait-for-it.sh
# RUN chmod +x /app/wait-for-it.sh

# Copy ETL script
COPY . /app

# Install cron
RUN apt-get update && apt-get install -y cron

# Add cron job 
RUN echo "0 */2 * * * root /app/etl_ingest.sh" >> /etc/crontab

# Start cron service and keep the container running
CMD cron -f
