# lambdas/processor/Dockerfile
FROM public.ecr.aws/lambda/python:3.12

# Install deps first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY handler.py ${LAMBDA_TASK_ROOT}

# Command is handler.handler (the Python function)
CMD ["handler.handler"]
