FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JELLYFIN_PROXY_PORT=8096 \
    JELLYFIN_CACHE_DIR=/cache

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

RUN mkdir -p /cache && chown -R app:app /app /cache

USER app

EXPOSE 8096
VOLUME ["/cache"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os,sys,urllib.request; port=os.getenv('JELLYFIN_PROXY_PORT', '8096'); url=f'http://127.0.0.1:{port}/proxy/cache/status';\
resp=urllib.request.urlopen(url, timeout=3); sys.exit(0 if getattr(resp, 'status', 200) == 200 else 1)"

CMD ["python", "main.py"]
