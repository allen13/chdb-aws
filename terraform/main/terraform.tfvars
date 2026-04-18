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

  test_users = {
    schema = [
      { name = "id", type = "long", required = true },
      { name = "email", type = "string", required = true },
      { name = "name", type = "string" },
      { name = "is_active", type = "boolean" },
      { name = "created_at", type = "timestamptz", required = true },
    ]
  }

  test_orders = {
    schema = [
      { name = "id", type = "long", required = true },
      { name = "user_id", type = "long", required = true },
      { name = "amount", type = "decimal(18,2)" },
      { name = "currency", type = "string" },
      { name = "order_ts", type = "timestamptz", required = true },
    ]
  }
}
