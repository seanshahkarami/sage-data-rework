# The hint publisher: catalog head (sqlite) -> version-hint.text (Pelican).
#
# Build context is `sage-data-rework/`. reconciler.py and the Pelican client
# it imports live in this directory.
#
# The directory is `reconciler_service/`, not `reconciler/`, so it cannot
# shadow `reconciler.py` on sys.path. A plain directory loses to a module
# today (it is only a namespace package).
#
# This image never opens a table, so it skips duckdb and pyarrow. It does
# carry pyiceberg -- see requirements.txt for why -- and the single writer
# of the hint stays easy to audit.
FROM python:3.11-slim

WORKDIR /app

COPY tiered_system/demo/reconciler_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tiered_system/demo/reconciler_service/pelican_fileio.py \
     tiered_system/demo/reconciler_service/pelican_token.py \
     tiered_system/demo/reconciler_service/reconciler.py \
     /app/reconciler_service/

ENV PYTHONPATH=/app/reconciler_service
ENV PYTHONUNBUFFERED=1

# Mounted at runtime, never baked in: this key signs for all of /sage.
ENV PELICAN_KEY_PATH=/secrets/issuer-key.pem

ENTRYPOINT ["python", "-u", "/app/reconciler_service/reconciler.py"]
