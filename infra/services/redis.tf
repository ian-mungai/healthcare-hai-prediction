locals {
  # Redis saves no snapshots and keeps no data on the host: a save forks the server and can need up to twice the data's memory under writes. The online
  # store is rebuilt by materialize from the offline features, so after a restart it is empty until the next materialize.
  redis_script = <<-EOT
    pw=$${REDIS_PASSWORD//\\/\\\\}
    pw=$${pw//\"/\\\"}
    exec redis-server - < <(
      printf '%s\n' 'bind 0.0.0.0' 'port 6379' 'dir /data' 'save ""' 'maxmemory 2048mb' 'maxmemory-policy noeviction'
      printf 'requirepass "%s"\n' "$pw"
    )
  EOT

  # While the model API is on, the Redis task also creates the read-only user of the model API from its own password (D18). Redis ACLs cannot bind a database,
  # so the user may run only the read commands the serving client sends and select databases 1 (the Feast hashes) and 2 (the marker). A connection starts in database 0,
  # which stays empty and which the user cannot write. The password is quoted like the default one. The octal escape 076 is the greater-than sign that starts an
  # ACL password rule, written so that no line holding the secret contains a redirection character.
  redis_script_with_model_api_user = <<-EOT
    pw=$${REDIS_PASSWORD//\\/\\\\}
    pw=$${pw//\"/\\\"}
    mp=$${MODEL_API_REDIS_PASSWORD//\\/\\\\}
    mp=$${mp//\"/\\\"}
    rules='resetkeys ~* resetchannels -@all +hmget +hget +hgetall +exists +ping +auth +hello +client|setname +select|1 +select|2'
    exec redis-server - < <(
      printf '%s\n' 'bind 0.0.0.0' 'port 6379' 'dir /data' 'save ""' 'maxmemory 2048mb' 'maxmemory-policy noeviction'
      printf 'requirepass "%s"\n' "$pw"
      printf 'user model_api on "\076%s" %s\n' "$mp" "$rules"
    )
  EOT

  redis_containers = [
    merge(local.container_defaults, {
      name              = "redis"
      image             = local.image["redis"]
      user              = "999:999"
      memory            = local.memory_profile.redis.limit
      memoryReservation = local.memory_profile.redis.reservation
      stopTimeout       = 60
      entryPoint        = ["bash", "-ec"]
      command           = [var.model_api_enabled ? local.redis_script_with_model_api_user : local.redis_script]
      secrets = concat(
        [
          { name = "REDIS_PASSWORD", valueFrom = local.secret_arn["redis_password"] },
          { name = "REDISCLI_AUTH", valueFrom = local.secret_arn["redis_password"] },
        ],
        var.model_api_enabled ? [{ name = "MODEL_API_REDIS_PASSWORD", valueFrom = local.secret_arn["model_api_redis_password"] }] : []
      )
      portMappings = [{ containerPort = 6379, hostPort = 6379, protocol = "tcp" }]
      # /data lives in task memory: a start never loads a dump left by an earlier task, so the store is empty until the next materialize.
      linuxParameters = { tmpfs = [{ containerPath = "/data", size = 64, mountOptions = ["nosuid", "nodev", "noexec"] }] }
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
