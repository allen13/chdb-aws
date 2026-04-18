import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    region: str
    data_bucket: str
    table_bucket_arn: str
    namespace: str
    result_format: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            region=os.environ["AWS_REGION"],
            data_bucket=os.environ["DATA_BUCKET"],
            table_bucket_arn=os.environ["TABLE_BUCKET_ARN"],
            namespace=os.environ["TABLE_NAMESPACE"],
            result_format=os.environ.get("READ_RESULT_FORMAT", "JSONCompact"),
        )
