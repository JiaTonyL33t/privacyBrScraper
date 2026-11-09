FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir requests "mcp>=1.9,<2"

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    TZ=America/Sao_Paulo

COPY main.py mcp_server.py /app/

# config.json, auth.json, the SQLite DB and content/ live in the mounted /data dir
WORKDIR /data
VOLUME ["/data"]

ENTRYPOINT ["python", "/app/main.py"]
CMD ["--help"]
