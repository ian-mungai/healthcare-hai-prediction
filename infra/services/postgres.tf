locals {
  # The session sends the role passwords as statements, so it stops reporting its activity first: pg_stat_activity then shows no statement text of it to a role with
  # pg_read_all_stats (pg_monitor includes it), which the monitoring role holds from the second start on. The connection user is the superuser, who may set it.
  postgres_databases_main = <<-EOT
    exec psql -X <<'SQL'
    \set ON_ERROR_STOP on
    \set VERBOSITY terse
    SET track_activities = off;
    SET log_statement = 'none';
    SET log_min_error_statement = 'panic';
    SET log_min_duration_statement = -1;
    \getenv airflow_password AIRFLOW_DB_PASSWORD
    CREATE SCHEMA IF NOT EXISTS polaris_schema;
    SELECT 'CREATE ROLE airflow LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') \gexec
    ALTER ROLE airflow PASSWORD :'airflow_password';
    SELECT 'CREATE DATABASE airflow OWNER airflow ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec
  EOT

  # The mlflow role and database exist only while mlflow_enabled is true, so the default definition stays unchanged. The statements repeat safely on every start.
  postgres_databases_mlflow = <<-EOT
    \getenv mlflow_password MLFLOW_DB_PASSWORD
    SELECT 'CREATE ROLE mlflow LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mlflow') \gexec
    ALTER ROLE mlflow PASSWORD :'mlflow_password';
    SELECT 'CREATE DATABASE mlflow OWNER mlflow ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow') \gexec
    SELECT 'CREATE DATABASE mlflow_auth OWNER mlflow ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow_auth') \gexec
  EOT

  # The OpenMetadata role and logical database exist only while openmetadata_persistent_enabled is true, so the default definition stays unchanged. Repeats are safe.
  postgres_databases_openmetadata = <<-EOT
    \getenv openmetadata_password OPENMETADATA_DB_PASSWORD
    SELECT 'CREATE ROLE openmetadata_user LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata_user') \gexec
    ALTER ROLE openmetadata_user PASSWORD :'openmetadata_password';
    SELECT 'CREATE DATABASE openmetadata_db OWNER openmetadata_user ENCODING ''UTF8''' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_db') \gexec
  EOT

  # The monitoring role exists only while monitoring_enabled is true, so the default definition stays unchanged. It may log in three times at once and holds the
  # built-in read-only monitoring role pg_monitor and nothing else (verified Nov 17 2026). The statements repeat safely on every start.
  postgres_databases_monitoring = <<-EOT
    \getenv exporter_password POSTGRES_EXPORTER_PASSWORD
    SELECT 'CREATE ROLE postgres_exporter LOGIN CONNECTION LIMIT 3' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres_exporter') \gexec
    ALTER ROLE postgres_exporter PASSWORD :'exporter_password';
    GRANT pg_monitor TO postgres_exporter;
  EOT

  postgres_databases_end = <<-EOT
    SQL
  EOT

  # While restore is off this is the script of every earlier revision. While it is on, scripts/pg_databases.sh creates the roles and each database and restores an empty one
  # from the dump that postgres_restore_fetch wrote.
  postgres_databases_script = local.pg_restore_on ? file("${path.module}/scripts/pg_databases.sh") : "${local.postgres_databases_main}${var.mlflow_enabled ? local.postgres_databases_mlflow : ""}${local.om_on ? local.postgres_databases_openmetadata : ""}${var.monitoring_enabled ? local.postgres_databases_monitoring : ""}${local.postgres_databases_end}"

  postgres_containers = concat([
    merge(local.container_defaults, {
      name              = "postgres"
      image             = local.image["postgres"]
      user              = "999:999"
      memory            = local.memory_profile.postgres.limit
      memoryReservation = local.memory_profile.postgres.reservation
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
      memory            = local.pg_restore_on ? 512 : 128
      memoryReservation = 64
      entryPoint        = ["sh", "-ec"]
      command           = [local.postgres_databases_script]
      environment = concat([
        { name = "PGCONNECT_TIMEOUT", value = "30" },
        { name = "PGDATABASE", value = "polaris" },
        { name = "PGHOST", value = "127.0.0.1" },
        { name = "PGUSER", value = "polaris" },
        ], local.pg_restore_on ? [
        { name = "PG_DATABASES", value = join(" ", local.postgres_database_pairs) },
        { name = "PG_ROLES", value = join(" ", local.postgres_role_pairs) },
        { name = "RESET_SCHEMAS", value = "polaris_schema" },
        { name = "RESTORE_DIR", value = "/restore" },
        { name = "RESTORE_SECONDS", value = "1200" },
      ] : [])
      secrets = concat([
        { name = "AIRFLOW_DB_PASSWORD", valueFrom = local.secret_arn["airflow_db_password"] },
        { name = "PGPASSWORD", valueFrom = local.secret_arn["postgres_password"] },
      ], var.mlflow_enabled ? [{ name = "MLFLOW_DB_PASSWORD", valueFrom = local.secret_arn["mlflow_db_password"] }] : [], local.om_on ? [{ name = "OPENMETADATA_DB_PASSWORD", valueFrom = local.secret_arn["openmetadata_db_password"] }] : [], var.monitoring_enabled ? [{ name = "POSTGRES_EXPORTER_PASSWORD", valueFrom = local.secret_arn["postgres_exporter_password"] }] : [])
      dependsOn        = concat([{ containerName = "postgres", condition = "HEALTHY" }], local.pg_restore_on ? [{ containerName = "postgres_restore_fetch", condition = "SUCCESS" }] : [])
      logConfiguration = local.log_configuration["postgres_databases"]
    }, local.pg_restore_on ? { mountPoints = [{ sourceVolume = "postgres_restore", containerPath = "/restore", readOnly = false }] } : {}),
  ], local.pg_restore_on ? [local.postgres_restore_fetch_container] : [])
}
