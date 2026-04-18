import os

import pytest


@pytest.fixture(autouse=True)
def _lambda_env(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("DATA_BUCKET", "chdb-aws-test-data")
    monkeypatch.setenv(
        "TABLE_BUCKET_ARN",
        "arn:aws:s3tables:us-east-1:000000000000:bucket/chdb-aws-test",
    )
    monkeypatch.setenv("TABLE_NAMESPACE", "analytics")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    yield
    for k in ("DATA_BUCKET", "TABLE_BUCKET_ARN", "TABLE_NAMESPACE"):
        os.environ.pop(k, None)
