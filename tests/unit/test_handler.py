from chdb_aws.handler import handler


def test_handler_returns_ok():
    result = handler({}, None)
    assert result["statusCode"] == 200
