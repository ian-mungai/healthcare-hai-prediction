locals {
  postgres_databases_main = <<-EOT
    exec psql -X <<'SQL'
    \set ON_ERROR_STOP on
    \set VERBOSITY terse
    SET log_statement = 'none';
    SET log_min_error_statement = 'panic';
    SET log_min_duration_statement = -1;
    \getenv airflow_password AIRFLOW_DB_PASSWORD
    \getenv marquez_password MARQUEZ_DB_PASSWORD
    CREATE SCHEMA IF NOT EXISTS polaris_schema;
    SELECT 'CREATE ROLE airflow LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') \gexec
    ALTER ROLE airflow PASSWORD :'airflow_password';
    SELECT 'CREATE DATABASE airflow OWNER airflow ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec
    SELECT 'CREATE ROLE marquez LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'marquez') \gexec
    ALTER ROLE marquez PASSWORD :'marquez_password';
    SELECT 'CREATE DATABASE marquez OWNER marquez ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'marquez') \gexec
  EOT

  # The mlflow role and database exist only while mlflow_enabled is true, so the default definition stays unchanged. The statements repeat safely on every start.
  postgres_databases_mlflow = <<-EOT
    \getenv mlflow_password MLFLOW_DB_PASSWORD
    SELECT 'CREATE ROLE mlflow LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mlflow') \gexec
    ALTER ROLE mlflow PASSWORD :'mlflow_password';
    SELECT 'CREATE DATABASE mlflow OWNER mlflow ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow') \gexec
    SELECT 'CREATE DATABASE mlflow_auth OWNER mlflow ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow_auth') \gexec
  EOT

  postgres_databases_end = <<-EOT
    SQL
  EOT

  postgres_databases_script = "${local.postgres_databases_main}${var.mlflow_enabled ? local.postgres_databases_mlflow : ""}${local.postgres_databases_end}"

  postgres_containers = [
    merge(local.container_defaults, {
      name              = "postgres"
      image             = local.image["postgres"]
      user              = "999:999"
      memory            = 2048
      memoryReservation = 1536
      stopTimeout       = 120
      command = [
        "postgres",
        "-c", "listen_addresses=127.0.0.1",
        "-c", "max_connections=200",
        "-c", "shared_buffers=512MB",
        "-c", "effective_cache_size=1GB",
      ]
      environment = [
        { name = "POSTGRES_DB", value = "polaris" },
        { name = "POSTGRES_INITDB_ARGS", value = "--encoding UTF8 --data-checksums" },
        { name = "POSTGRES_USER", value = "polaris" },
      ]
      secrets      = [{ name = "POSTGRES_PASSWORD", valueFrom = local.secret_arn["postgres_password"] }]
      portMappings = [{ containerPort = 5432, hostPort = 5432, protocol = "tcp" }]
      mountPoints  = [{ sourceVolume = "postgres_data", containerPath = "/var/lib/postgresql/data", readOnly = false }]
      linuxParameters = {
        sharedMemorySize = 256
      }
      healthCheck = {
        command     = ["CMD-SHELL", "pg_isready -h 127.0.0.1 -U polaris -d polaris"]
        interval    = 10
        timeout     = 5
        retries     = 10
        startPeriod = 60
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["postgres"]
    }),
    merge(local.container_defaults, {
      name              = "postgres_databases"
      image             = local.image["postgres"]
      user              = "999:999"
      essential         = false
      memory            = 128
      memoryReservation = 64
      entryPoint        = ["sh", "-ec"]
      command           = [local.postgres_databases_script]
      environment = [
        { name = "PGCONNECT_TIMEOUT", value = "30" },
        { name = "PGDATABASE", value = "polaris" },
        { name = "PGHOST", value = "127.0.0.1" },
        { name = "PGUSER", value = "polaris" },
      ]
      secrets = concat([
        { name = "AIRFLOW_DB_PASSWORD", valueFrom = local.secret_arn["airflow_db_password"] },
        { name = "MARQUEZ_DB_PASSWORD", valueFrom = local.secret_arn["marquez_db_password"] },
        { name = "PGPASSWORD", valueFrom = local.secret_arn["postgres_password"] },
      ], var.mlflow_enabled ? [{ name = "MLFLOW_DB_PASSWORD", valueFrom = local.secret_arn["mlflow_db_password"] }] : [])
      dependsOn        = [{ containerName = "postgres", condition = "HEALTHY" }]
      logConfiguration = local.log_configuration["postgres_databases"]
    }),
  ]
}
