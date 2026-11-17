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
  # so the user may run only the read commands the serving client sends and select databases 1 (the Feast hashes of the single path), 2 (the markers) and 3 to 5 (the Feast hashes of the online model sets m1, m2 and m4). A connection starts in database 0,
  # which stays empty and which the user cannot write. The password is quoted like the default one. The octal escape 076 is the greater-than sign that starts an
  # ACL password rule, written so that no line holding the secret contains a redirection character.
  redis_script_with_model_api_user = <<-EOT
    pw=$${REDIS_PASSWORD//\\/\\\\}
    pw=$${pw//\"/\\\"}
    mp=$${MODEL_API_REDIS_PASSWORD//\\/\\\\}
    mp=$${mp//\"/\\\"}
    rules='resetkeys ~* resetchannels -@all +hmget +hget +hgetall +exists +ping +auth +hello +client|setname +select|1 +select|2 +select|3 +select|4 +select|5'
    exec redis-server - < <(
      printf '%s\n' 'bind 0.0.0.0' 'port 6379' 'dir /data' 'save ""' 'maxmemory 2048mb' 'maxmemory-policy noeviction'
      printf 'requirepass "%s"\n' "$pw"
      printf 'user model_api on "\076%s" %s\n' "$mp" "$rules"
    )
  EOT

  # While monitoring is on, the Redis task also creates the user exporter of the Redis exporter, from its own password. It may run only what the exporter
  # needs (verified Nov 17 2026: info, ping, client list and setname, slowlog, latency, command info and dbsize) and no key or channel. It has no CONFIG command, because CONFIG GET returns the requirepass of the default user in clear text; the exporter collects no config (--config-command=-), and redis_memory_max_bytes still comes from INFO. The script
  # is written for each of the two cases of the model API user, so the definitions without monitoring stay as they were.
  redis_exporter_rules = "resetkeys resetchannels -@all +info +ping +client|list +client|setname +slowlog|get +slowlog|len +latency|latest +latency|histogram +command|info +dbsize"

  redis_script_with_exporter_user = <<-EOT
    pw=$${REDIS_PASSWORD//\\/\\\\}
    pw=$${pw//\"/\\\"}
    ex=$${EXPORTER_REDIS_PASSWORD//\\/\\\\}
    ex=$${ex//\"/\\\"}
    exrules='${local.redis_exporter_rules}'
    exec redis-server - < <(
      printf '%s\n' 'bind 0.0.0.0' 'port 6379' 'dir /data' 'save ""' 'maxmemory 2048mb' 'maxmemory-policy noeviction'
      printf 'requirepass "%s"\n' "$pw"
      printf 'user exporter on "\076%s" %s\n' "$ex" "$exrules"
    )
  EOT

  redis_script_with_both_users = <<-EOT
    pw=$${REDIS_PASSWORD//\\/\\\\}
    pw=$${pw//\"/\\\"}
    mp=$${MODEL_API_REDIS_PASSWORD//\\/\\\\}
    mp=$${mp//\"/\\\"}
    ex=$${EXPORTER_REDIS_PASSWORD//\\/\\\\}
    ex=$${ex//\"/\\\"}
    rules='resetkeys ~* resetchannels -@all +hmget +hget +hgetall +exists +ping +auth +hello +client|setname +select|1 +select|2 +select|3 +select|4 +select|5'
    exrules='${local.redis_exporter_rules}'
    exec redis-server - < <(
      printf '%s\n' 'bind 0.0.0.0' 'port 6379' 'dir /data' 'save ""' 'maxmemory 2048mb' 'maxmemory-policy noeviction'
      printf 'requirepass "%s"\n' "$pw"
      printf 'user model_api on "\076%s" %s\n' "$mp" "$rules"
      printf 'user exporter on "\076%s" %s\n' "$ex" "$exrules"
    )
  EOT

  redis_script_chosen = (
    var.monitoring_enabled
    ? (var.model_api_enabled ? local.redis_script_with_both_users : local.redis_script_with_exporter_user)
    : (var.model_api_enabled ? local.redis_script_with_model_api_user : local.redis_script)
  )

  redis_containers = [
    merge(local.container_defaults, {
      name              = "redis"
      image             = local.image["redis"]
      user              = "999:999"
      memory            = local.memory_profile.redis.limit
      memoryReservation = local.memory_profile.redis.reservation
      stopTimeout       = 60
      entryPoint        = ["bash", "-ec"]
      command           = [local.redis_script_chosen]
      secrets = concat(
        [
          { name = "REDIS_PASSWORD", valueFrom = local.secret_arn["redis_password"] },
          { name = "REDISCLI_AUTH", valueFrom = local.secret_arn["redis_password"] },
        ],
        var.model_api_enabled ? [{ name = "MODEL_API_REDIS_PASSWORD", valueFrom = local.secret_arn["model_api_redis_password"] }] : [],
        var.monitoring_enabled ? [{ name = "EXPORTER_REDIS_PASSWORD", valueFrom = local.secret_arn["redis_exporter_password"] }] : []
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
