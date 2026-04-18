import boto3
import pytest
from moto import mock_aws

from chdb_aws.write.archiver import ARCHIVE_TAG, archive

BUCKET = "chdb-aws-test-data"
SRC = "assets/events/dropzone/file.parquet"
DST = "assets/events/archive/file.parquet"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        client.put_object(Bucket=BUCKET, Key=SRC, Body=b"payload")
        yield client


def test_archive_moves_and_tags(s3):
    dst = archive(s3, BUCKET, SRC)
    assert dst == DST

    assert s3.get_object(Bucket=BUCKET, Key=DST)["Body"].read() == b"payload"
    with pytest.raises(s3.exceptions.NoSuchKey):
        s3.get_object(Bucket=BUCKET, Key=SRC)

    tagging = s3.get_object_tagging(Bucket=BUCKET, Key=DST)["TagSet"]
    assert {"Key": "lifecycle", "Value": "archived"} in tagging
    assert ARCHIVE_TAG == "lifecycle=archived"


def test_archive_rejects_non_dropzone_key(s3):
    with pytest.raises(ValueError, match="dropzone"):
        archive(s3, BUCKET, "assets/events/elsewhere/file.parquet")
