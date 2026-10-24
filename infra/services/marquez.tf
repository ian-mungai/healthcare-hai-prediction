locals {
  marquez_config = <<-EOT
    server:
      applicationConnectors:
        - type: http
          bindHost: 127.0.0.1
          port: 5000
          httpCompliance: RFC7230_LEGACY
      adminConnectors:
        - type: http
          bindHost: 127.0.0.1
          port: 5001
    db:
      driverClass: org.postgresql.Driver
      url: jdbc:postgresql://127.0.0.1:5432/marquez
      user: marquez
      password: $${MARQUEZ_DB_PASSWORD}
    migrateOnStartup: true
    graphql:
      enabled: true
    search:
      enabled: false
    logging:
      level: INFO
      appenders:
        - type: console
  EOT

  marquez_script = <<-EOT
    cat > /tmp/marquez.yaml <<'YAML'
    ${local.marquez_config}
    YAML
    export MARQUEZ_CONFIG=/tmp/marquez.yaml
    exec /usr/src/app/entrypoint.sh
  EOT

  marquez_containers = [for container in [
    merge(local.container_defaults, {
      name              = "marquez_api"
      image             = lookup(local.image, "marquez", "")
      user              = "10001:10001"
      memory            = 1280
      memoryReservation = 768
      stopTimeout       = 60
      entryPoint        = ["bash", "-ec"]
      command           = [local.marquez_script]
      environment       = [{ name = "JAVA_OPTS", value = "-XX:MaxRAMPercentage=70" }]
      secrets           = [{ name = "MARQUEZ_DB_PASSWORD", valueFrom = local.secret_arn["marquez_db_password"] }]
      portMappings = [
        { containerPort = 5000, hostPort = 5000, protocol = "tcp" },
        { containerPort = 5001, hostPort = 5001, protocol = "tcp" },
      ]
      healthCheck = {
        command     = ["CMD", "bash", "-ec", "exec 3<>/dev/tcp/127.0.0.1/5001; printf 'GET /healthcheck HTTP/1.0\\r\\nHost: localhost\\r\\n\\r\\n' >&3; read -r status <&3; [[ $status == *' 200 '* ]]"]
        interval    = 30
        timeout     = 5
        retries     = 10
        startPeriod = 60
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["marquez_api"]
    }),
    merge(local.container_defaults, {
      name              = "marquez_web"
      image             = lookup(local.image, "marquez_web", "")
      user              = "1000:1000"
      memory            = 256
      memoryReservation = 128
      environment = [
        { name = "MARQUEZ_HOST", value = "127.0.0.1" },
        { name = "MARQUEZ_PORT", value = "5000" },
        { name = "REACT_APP_ADVANCED_SEARCH", value = "false" },
        { name = "WEB_HOST", value = "127.0.0.1" },
        { name = "WEB_PORT", value = "3000" },
      ]
      portMappings = [{ containerPort = 3000, hostPort = 3000, protocol = "tcp" }]
      dependsOn    = [{ containerName = "marquez_api", condition = "HEALTHY" }]
      healthCheck = {
        command     = ["CMD", "node", "-e", "const r=require('http').get('http://127.0.0.1:3000/healthcheck',s=>process.exit(s.statusCode===200?0:1));r.setTimeout(2000,()=>{r.destroy();process.exit(1)});r.on('error',()=>process.exit(1));"]
        interval    = 15
        timeout     = 5
        retries     = 10
        startPeriod = 20
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["marquez_web"]
    }),
  ] : container if var.marquez_enabled]
}
