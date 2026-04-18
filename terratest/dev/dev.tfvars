aws_region        = "us-east-1"
environment       = "dev"
table_bucket_name = "chdb-aws-dev"
table_namespace   = "analytics_dev"

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
