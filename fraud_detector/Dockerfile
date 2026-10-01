FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY scripts ./scripts
COPY artifacts ./artifacts

CMD ["bash", "-c", "python scripts/preprocess.py & python scripts/scorer.py & wait -n; exit 1"]
