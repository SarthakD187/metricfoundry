FROM public.ecr.aws/lambda/python:3.12

# Install deps first to leverage Docker layer caching
COPY lambdas/processor/requirements.txt .
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY services ${LAMBDA_TASK_ROOT}/services
COPY lambdas/processor/handler.py ${LAMBDA_TASK_ROOT}

CMD ["handler.handler"]