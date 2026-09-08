# Your hedger goes here. Replace this file (and add your source) so that the
# container builds and runs your hedger (see TASK.md, Job 2).
#
# Requirements:
#   - Connect to the exchange's NATS at $NATS_URL
#     (inside the compose network that is nats://nats:4222).
#   - Trade under the sender id in $HEDGER_SENDER (default fine for dev).
#   - Run headless as the container's CMD/ENTRYPOINT (no host dependencies).
#
# Any language. Python is allowed for the hedger.
#
# Placeholder so `docker compose --profile strategy up` works before you start:

# syntax=docker/dockerfile:1

FROM --platform=linux/amd64 python:3.11-slim
 
WORKDIR /app
RUN pip install --no-cache-dir nats-py
COPY hedger.py .
 
ENTRYPOINT ["python3", "hedger.py"]
 