FROM public.ecr.aws/lambda/python:3.12

# Install heavyweight analytics dependencies required by the processor.
COPY lambdas/processor/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf ~/.cache/pip

# Copy the shared services code and the processor handler into the image.
COPY services ${LAMBDA_TASK_ROOT}/services
COPY lambdas/processor ${LAMBDA_TASK_ROOT}/

CMD ["handler.main"]
