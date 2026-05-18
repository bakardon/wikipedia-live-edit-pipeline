FROM apache/spark:3.5.3

USER root
WORKDIR /app

# apache/spark image is python-capable but pip may not be present; install if missing.
RUN python3 -m pip --version >/dev/null 2>&1 || \
    (apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*)

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    (python3 -m pip install --timeout 120 --retries 12 --break-system-packages -r requirements.txt 2>/dev/null \
     || python3 -m pip install --timeout 120 --retries 12 -r requirements.txt)

COPY . .

# Connector jars resolved at runtime via --packages (Ivy cache persists in image layer).
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "--master", "local[2]", \
     "--driver-memory", "2g", \
     "/app/stream_job.py"]
