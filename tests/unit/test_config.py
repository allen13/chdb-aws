from chdb_aws.config import Config


def test_from_env_reads_required_vars():
    cfg = Config.from_env()
    assert cfg.region == "us-east-1"
    assert cfg.data_bucket == "chdb-aws-test-data"
    assert cfg.namespace == "analytics"
    assert cfg.table_bucket_arn.endswith("/chdb-aws-test")
    assert cfg.result_format == "JSONCompact"


def test_result_format_override(monkeypatch):
    monkeypatch.setenv("READ_RESULT_FORMAT", "Pretty")
    assert Config.from_env().result_format == "Pretty"
