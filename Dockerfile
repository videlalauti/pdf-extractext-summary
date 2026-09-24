FROM python:3.14-slim-bookworm

RUN useradd --create-home --shell /bin/bash --uid 1000 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py app.py routes.py .

EXPOSE 8000

USER appuser

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]