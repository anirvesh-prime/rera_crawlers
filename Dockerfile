FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0 \
    RERA_IN_DOCKER=true \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_BIN=/usr/bin/chromedriver

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        chromium \
        chromium-driver \
        fonts-liberation \
        fonts-noto-core \
        fonts-noto-color-emoji \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "-u", "run_crawlers.py"]
