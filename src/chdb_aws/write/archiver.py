ARCHIVE_TAG = "lifecycle=archived"


def archive(s3_client, bucket: str, src_key: str) -> str:
    if "/dropzone/" not in src_key:
        raise ValueError(f"key {src_key!r} is not under a dropzone/ prefix")

    dst_key = src_key.replace("/dropzone/", "/archive/", 1)

    s3_client.copy_object(
        Bucket=bucket,
        Key=dst_key,
        CopySource={"Bucket": bucket, "Key": src_key},
        Tagging=ARCHIVE_TAG,
        TaggingDirective="REPLACE",
    )
    s3_client.delete_object(Bucket=bucket, Key=src_key)
    return dst_key
