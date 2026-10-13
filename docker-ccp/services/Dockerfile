FROM python:3.12-slim

WORKDIR /app

# Instalar dependencias de compilación mínimas para paquetes
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

EXPOSE 8000 8005

CMD ["python", "sales_service.py"]
