import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

BUCKET = "chdb-aws-test-data"


def _put_parquet(s3, key: str) -> pa.Table:
    table = pa.table({"id": ["a", "b"], "value": [1, 2]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return table


@pytest.fixture
def s3_and_handler(monkeypatch):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)

        # Import inside fixture so module-scope boto3 client uses the mock.
        from chdb_aws.write import handler as write_handler

        monkeypatch.setattr(write_handler, "_s3", s3)

        captured: dict = {}

        def fake_append(cfg, asset, rows):
            captured["asset"] = asset
            captured["rows"] = rows

        monkeypatch.setattr(write_handler.iceberg_writer, "append", fake_append)
        yield s3, write_handler, captured


def _event(key: str) -> dict:
    return {"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": key}}}]}


def test_handler_routes_appends_and_archives(s3_and_handler):
    s3, write_handler, captured = s3_and_handler
    src = "assets/events/dropzone/file.parquet"
    expected = _put_parquet(s3, src)

    result = write_handler.handler(_event(src), None)

    assert result == {"processed": [src]}
    assert captured["asset"] == "events"
    assert captured["rows"].equals(expected)

    dst = "assets/events/archive/file.parquet"
    assert s3.get_object(Bucket=BUCKET, Key=dst)["Body"].read()
    with pytest.raises(s3.exceptions.NoSuchKey):
        s3.get_object(Bucket=BUCKET, Key=src)


def test_handler_skips_non_dropzone_keys(s3_and_handler):
    _, write_handler, captured = s3_and_handler
    result = write_handler.handler(
        _event("assets/events/archive/something.parquet"), None
    )
    assert result == {"processed": []}
    assert captured == {}
