FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/data-pipeline-ai

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/data-pipeline-ai/requirements.txt
RUN pip install --upgrade pip && pip install -r /opt/data-pipeline-ai/requirements.txt

COPY . /opt/data-pipeline-ai

ENV PYTHONPATH=/opt/data-pipeline-ai

EXPOSE 8000

# Default to the API service; compose overrides this for other containers.
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
