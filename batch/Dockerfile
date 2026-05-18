FROM apache/spark:3.5.3

USER root
WORKDIR /app

RUN python3 -m pip --version >/dev/null 2>&1 || \
    (apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*)

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    (python3 -m pip install --timeout 120 --retries 12 --break-system-packages -r requirements.txt 2>/dev/null \
     || python3 -m pip install --timeout 120 --retries 12 -r requirements.txt)

COPY . .

# local[2] + modest driver heap: leaves cgroup RAM for gzip/CSV native buffers and JDBC batches
# (local[*] + 4g driver OOMs on default 5g batch container limits).
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.postgresql:postgresql:42.7.3", \
     "--master", "local[2]", \
     "--driver-memory", "2g", \
     "/app/clickstream_load.py"]
