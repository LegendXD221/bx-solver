# bx-solver on Render (Docker runtime, free plan compatible).
# The image carries the patched chromium — RAM at RUNTIME is what the free
# plan caps (512MB), and the BX_RENDER_FREE=1 env keeps the footprint inside
# it. Build deps (apt libs + ~170MB browser) cost image size only.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/opt/bx-solver/browsers

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m patchright install --with-deps chromium

COPY bx_solver.py ./

# Render injects $PORT and expects the service to bind it (bx_solver.py
# honors PORT first, BX_SOLVER_PORT second, 8787 fallback).
EXPOSE 8787
CMD ["python", "bx_solver.py"]
