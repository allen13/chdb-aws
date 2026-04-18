FROM public.ecr.aws/lambda/python:3.13 AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project \
    && cp -r .venv/lib/python*/site-packages/. ${LAMBDA_TASK_ROOT}/ \
    && rm -rf .venv

COPY src/ ${LAMBDA_TASK_ROOT}/

CMD ["chdb_aws.write.handler.handler"]
