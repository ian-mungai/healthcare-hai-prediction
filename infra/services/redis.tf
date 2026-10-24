locals {
  redis_script = <<-EOT
    pw=$${REDIS_PASSWORD//\\/\\\\}
    pw=$${pw//\"/\\\"}
    exec redis-server - < <(
      printf '%s\n' 'bind 0.0.0.0' 'port 6379' 'dir /data' 'save 900 1' 'save 300 100' 'maxmemory 640mb' 'maxmemory-policy noeviction'
      printf 'requirepass "%s"\n' "$pw"
    )
  EOT

  redis_containers = [
    merge(local.container_defaults, {
      name              = "redis"
      image             = local.image["redis"]
      user              = "999:999"
      memory            = 1024
      memoryReservation = 512
      stopTimeout       = 60
      entryPoint        = ["bash", "-ec"]
      command           = [local.redis_script]
      secrets = [
        { name = "REDIS_PASSWORD", valueFrom = local.secret_arn["redis_password"] },
        { name = "REDISCLI_AUTH", valueFrom = local.secret_arn["redis_password"] },
      ]
      portMappings = [{ containerPort = 6379, hostPort = 6379, protocol = "tcp" }]
      mountPoints  = [{ sourceVolume = "redis_data", containerPath = "/data", readOnly = false }]
      healthCheck = {
        command     = ["CMD-SHELL", "redis-cli -h 127.0.0.1 ping | grep -q PONG"]
        interval    = 10
        timeout     = 5
        retries     = 5
        startPeriod = 15
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["redis"]
    }),
  ]
}
