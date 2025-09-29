FROM python:3.10-slim-bullseye

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Установка системных зависимостей и браузеров
RUN playwright install-deps
RUN playwright install chromium

COPY main.py .
COPY avito_processor.py .
COPY services/ ./services/

RUN mkdir -p database trash

CMD ["python", "main.py"]