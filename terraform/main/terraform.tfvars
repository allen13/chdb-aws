aws_region        = "us-east-1"
environment       = "prod"
table_bucket_name = "chdb-aws"
table_namespace   = "analytics"

assets = {
  events = {
    schema = [
      { name = "id", type = "string", required = true },
      { name = "event_type", type = "string" },
      { name = "ts", type = "timestamp" },
      { name = "payload", type = "string" },
    ]
  }
}
