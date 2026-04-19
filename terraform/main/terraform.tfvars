aws_region        = "us-east-1"
environment       = "prod"
table_bucket_name = "chdb-aws"
table_namespace   = "analytics"

assets = {
  # CDN edge request log — single asset used by the demo scripts under
  # scripts/demo/. Chosen to exercise a broad slice of chDB's query surface
  # (time bucketing, quantiles, approx distinct, top-K, regex, windows).
  requests = {
    schema = [
      { name = "request_id", type = "string", required = true },
      { name = "ts", type = "timestamptz", required = true },
      { name = "edge_pop", type = "string" },
      { name = "client_ip", type = "string" },
      { name = "country", type = "string" },
      { name = "http_method", type = "string" },
      { name = "path", type = "string" },
      { name = "status_code", type = "int" },
      { name = "bytes_sent", type = "long" },
      { name = "response_time_ms", type = "int" },
      { name = "cache_hit", type = "boolean" },
      { name = "user_agent", type = "string" },
      { name = "referrer", type = "string" },
    ]
  }
}
