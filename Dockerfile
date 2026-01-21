# ── Stage 1: build + runtime ──────────────────────────────────────────────
FROM python:3.11-slim-bullseye

# 1. basic hygiene
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 2. install deps first (leverages Docker layer cache)
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 3. copy source *after* deps so code edits don’t bust the cache
COPY . .

# 4. editable install (so import paths match dev machine)
RUN pip install -e .

# 5. default entry-point = your CLI
#    `docker run image train` or `docker run image fullrun`
ENTRYPOINT ["python", "-m", "bball.cli"]
CMD ["--help"]
