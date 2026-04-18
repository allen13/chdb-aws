import pytest


@pytest.fixture
def patched(monkeypatch):
    from chdb_aws.read import handler as read_handler
    from chdb_aws.read import query as read_query

    captured = {}

    def fake_get(**kwargs):
        captured["lookup"] = kwargs
        return {
            "metadataLocation": (
                "s3://warehouse-bucket/abc/table-events/"
                "metadata/00007-uuid.metadata.json"
            )
        }

    def fake_chdb_query(sql, fmt):
        captured["sql"] = sql
        captured["fmt"] = fmt
        return '{"rows":[[42]]}'

    monkeypatch.setattr(read_query._s3tables, "get_table_metadata_location", fake_get)
    monkeypatch.setattr(read_query.chdb, "query", fake_chdb_query)
    return read_handler, captured


def test_handler_substitutes_table_fn_and_settings(patched):
    handler, captured = patched
    event = {"asset": "events", "sql": "SELECT count() FROM ${asset}"}

    out = handler.handler(event, None)

    assert out["statusCode"] == 200
    assert out["body"] == '{"rows":[[42]]}'

    assert captured["lookup"]["name"] == "events"
    assert captured["lookup"]["namespace"] == "analytics"

    sql = captured["sql"]
    assert "icebergS3('s3://warehouse-bucket/abc/table-events/')" in sql
    assert (
        "SETTINGS iceberg_metadata_file_path = "
        "'metadata/00007-uuid.metadata.json'" in sql
    )
    assert captured["fmt"] == "JSONCompact"
