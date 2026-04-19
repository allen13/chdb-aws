locals {
  name_prefix = "${var.project_name}-${var.environment}"

  # Iceberg primitive type → AWS Glue (Hive) type. Used to render
  # storage_descriptor.columns when registering the Glue Iceberg tables.
  # Decimal/fixed pass through verbatim because Glue accepts the same form.
  glue_type_map = {
    boolean     = "boolean"
    int         = "int"
    long        = "bigint"
    float       = "float"
    double      = "double"
    date        = "date"
    time        = "string" # Glue has no time type; store as string
    timestamp   = "timestamp"
    timestamptz = "timestamp"
    string      = "string"
    uuid        = "string"
    binary      = "binary"
  }

  # Translate each asset's Iceberg field list to a Glue-friendly column list.
  # Anything we don't have an exact match for is left as-is (decimal(p,s),
  # fixed(N), nested types) — Glue accepts those literal strings.
  glue_columns = {
    for asset, def in var.assets : asset => [
      for f in def.schema : {
        name = f.name
        type = lookup(local.glue_type_map, f.type, f.type)
      }
    ]
  }
}
