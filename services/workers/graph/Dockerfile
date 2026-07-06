# services/workers/graph/Dockerfile
FROM public.ecr.aws/docker/library/python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install system dependencies needed for scientific stack
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       curl \
       git \
       libpq-dev \
       libssl-dev \
       libffi-dev \
       libxml2-dev \
       libxslt1-dev \
       libjpeg62-turbo-dev \
       zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip \
    && python -m pip install awslambdaric -r requirements.txt

# Copy application source
COPY services ./services
COPY lambdas ./lambdas

ENV PYTHONPATH=/app

EXPOSE 8080

CMD ["python", "-m", "awslambdaric", "services.workers.graph.app.lambda_handler"]
