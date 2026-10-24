locals {
  polaris_containers = [
    merge(local.container_defaults, {
      name              = "polaris"
      image             = local.image["polaris"]
      user              = "10000:10001"
      memory            = 2048
      memoryReservation = 1536
      stopTimeout       = 60
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE", value = "postgresql" },
        { name = "POLARIS_PERSISTENCE_TYPE", value = "relational-jdbc" },
        { name = "POLARIS_REALM_CONTEXT_REALMS", value = "POLARIS" },
        { name = "QUARKUS_DATASOURCE_JDBC_URL", value = "jdbc:postgresql://127.0.0.1:5432/polaris" },
        { name = "QUARKUS_DATASOURCE_USERNAME", value = "polaris" },
        { name = "QUARKUS_MANAGEMENT_HOST", value = "127.0.0.1" },
        { name = "QUARKUS_OTEL_SDK_DISABLED", value = "true" },
      ]
      secrets = [{ name = "QUARKUS_DATASOURCE_PASSWORD", valueFrom = local.secret_arn["postgres_password"] }]
      portMappings = [
        { containerPort = 8181, hostPort = 8181, protocol = "tcp" },
        { containerPort = 8182, hostPort = 8182, protocol = "tcp" },
      ]
      healthCheck = {
        command     = ["CMD", "curl", "--fail", "--silent", "http://127.0.0.1:8182/q/health"]
        interval    = 10
        timeout     = 10
        retries     = 10
        startPeriod = 60
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["polaris"]
    }),
  ]
}
