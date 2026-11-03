# syntax=docker/dockerfile:1.7

FROM python:3.13-slim-trixie AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

FROM build AS deps
WORKDIR /app
COPY requirements.txt requirements.lock.txt ./
RUN ln -sf /usr/local/bin/python /usr/bin/python \
 && uv venv --python /usr/bin/python /opt/venv \
 && uv pip install --python /opt/venv/bin/python -r requirements.lock.txt

FROM gcr.io/distroless/python3-debian13:nonroot@sha256:64e54c4b1200da244d80181d98e86fc01d73c8101f107d5baf472152d8e119a2
WORKDIR /app
COPY --from=deps /opt/venv /opt/venv
COPY main.py app.py routes.py cache.py consumer.py errors.py error_handlers.py llm.py entrypoint.py ./
ENV PATH="/opt/venv/bin:$PATH"
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/bin/python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ.get('PORT', '8000') + '/health', timeout=4)"]
ENTRYPOINT ["/opt/venv/bin/python"]
CMD ["entrypoint.py"]
