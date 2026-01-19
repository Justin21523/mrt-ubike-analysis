FROM python:3.10-slim

WORKDIR /app

# System deps: keep minimal; add curl for optional healthchecks/debug.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
COPY requirements-dev.txt requirements-dev.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV METROBIKEATLAS_DEMO_MODE=false

EXPOSE 8000

CMD ["python", "scripts/run_api.py"]

