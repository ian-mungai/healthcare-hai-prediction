variable "openmetadata_enabled" {
  description = "Register the on-demand OpenMetadata catalog task that the broker starts for metadata_ingest_dag. Off by default. It needs the four catalog image digests, the execution and catalog task roles and the jobs log group."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.openmetadata_enabled || var.log_group_name == "${var.project_name}_jobs_${var.environment}"
    error_message = "The catalog task needs the merged platform jobs log group."
  }
}

variable "openmetadata_images" {
  description = "Server, ingestion, search and database image URIs from the project ECR repositories <project>_openmetadata_<name>_<environment>, pinned by digest. All four must list the platform of openmetadata_cpu_architecture."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, image in var.openmetadata_images :
        contains(["server", "ingestion", "search", "database"], name) &&
        can(regex("^${var.expected_account_id}\\.dkr\\.ecr\\.${var.aws_region}\\.amazonaws\\.com/${var.project_name}_openmetadata_${name}_${var.environment}@sha256:[a-f0-9]{64}$", image))
      ]) &&
      (!var.openmetadata_enabled || alltrue([for name in ["server", "ingestion", "search", "database"] : contains(keys(var.openmetadata_images), name)]))
    )
    error_message = "Supply the server, ingestion, search and database images from their project ECR repositories with an immutable SHA-256 digest."
  }
}

variable "openmetadata_roles" {
  description = "The merged IAM role ARNs of the catalog task, keyed execution and openmetadata_task."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, arn in var.openmetadata_roles :
        contains(["execution", "openmetadata_task"], name) && arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_${name}_${var.environment}"
      ]) &&
      (!var.openmetadata_enabled || alltrue([for name in ["execution", "openmetadata_task"] : contains(keys(var.openmetadata_roles), name)]))
    )
    error_message = "Bind only the exact merged execution role and catalog task role. A writer or another job's role is refused."
  }
}

variable "openmetadata_resources" {
  description = "Fargate CPU units, memory MiB and ephemeral storage GiB of the catalog task. The default is 4 vCPU and 24 GiB."
  type = object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  })
  default = { cpu = 4096, memory = 24576, ephemeral_storage_gib = 30 }

  validation {
    condition = try(
      var.openmetadata_resources.memory >= local.memory_shapes[tostring(var.openmetadata_resources.cpu)].min &&
      var.openmetadata_resources.memory <= local.memory_shapes[tostring(var.openmetadata_resources.cpu)].max &&
      var.openmetadata_resources.memory % local.memory_shapes[tostring(var.openmetadata_resources.cpu)].step == 0 &&
      var.openmetadata_resources.ephemeral_storage_gib >= 20 && var.openmetadata_resources.ephemeral_storage_gib <= 200 &&
      var.openmetadata_resources.ephemeral_storage_gib == floor(var.openmetadata_resources.ephemeral_storage_gib), false
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch."
  }
}

variable "openmetadata_lifetime_seconds" {
  description = "Seconds the catalog stays up after the task starts, then the task stops itself. The DAG's limit follows the ceiling of four hours (LIFETIME_CEILING_SECONDS in dags/metadata_ingest_dag.py)."
  type        = number
  default     = 7200
  nullable    = false

  validation {
    condition     = var.openmetadata_lifetime_seconds >= 1800 && var.openmetadata_lifetime_seconds <= 14400 && var.openmetadata_lifetime_seconds == floor(var.openmetadata_lifetime_seconds)
    error_message = "Use a whole number of seconds from 1800 to 14400. The task must end by itself within four hours."
  }
}

variable "openmetadata_cpu_architecture" {
  description = "CPU architecture of the catalog task. X86_64 until the owner has checked that all four pushed images list linux/arm64, then ARM64, which costs about 20 percent less."
  type        = string
  default     = "X86_64"
  nullable    = false

  validation {
    condition     = contains(["X86_64", "ARM64"], var.openmetadata_cpu_architecture)
    error_message = "Use X86_64 or ARM64."
  }
}

variable "openmetadata_marquez_url" {
  description = "Private http address of Marquez on the container host, for example http://10.0.1.20:5000. Empty leaves out the Marquez step. Marquez must listen on a non-loopback address and the IAM stack must open the port."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = var.openmetadata_marquez_url == "" || can(regex("^http://[0-9]{1,3}(\\.[0-9]{1,3}){3}:[0-9]{2,5}$", var.openmetadata_marquez_url))
    error_message = "Use an http address with a private IPv4 address and a port, or leave it empty."
  }
}

variable "openmetadata_admin_password_name" {
  description = "Parameter Store SecureString name of the catalog administrator password. The bootstrap container receives it. Terraform references the name and never reads the value."
  type        = string
  default     = "/healthcare_hai_prediction/openmetadata_admin_password"
  nullable    = false

  validation {
    condition     = !var.openmetadata_enabled || var.openmetadata_admin_password_name == "/${var.project_name}/openmetadata_admin_password"
    error_message = "Inject only the project catalog administrator password parameter."
  }
}

variable "openmetadata_commands" {
  description = "Start commands of the upstream images, unproven until the owner reads each image's entry point. The wrapper runs server, search and database and the migrate command runs once."
  type = object({
    server   = list(string)
    migrate  = list(string)
    search   = list(string)
    database = list(string)
  })
  default = {
    server   = ["./openmetadata-start.sh"]
    migrate  = ["./bootstrap/openmetadata-ops.sh", "migrate"]
    search   = ["./opensearch-docker-entrypoint.sh", "opensearch"]
    database = ["docker-entrypoint.sh", "postgres"]
  }
  nullable = false
}

locals {
  om_repo    = "${path.module}/../.."
  om_extra   = 120
  om_bounded = file("${local.om_repo}/scripts/infrastructure/openmetadata_bounded_run.sh")
  om_migrate = file("${local.om_repo}/scripts/infrastructure/openmetadata_migrate.sh")
  # Files the init step writes into the workspace, so the task runs the tracked scripts and configuration unchanged and a change to either makes a new revision.
  om_files = {
    "bin/openmetadata_ingest.py"    = file("${local.om_repo}/scripts/infrastructure/openmetadata_ingest.py")
    "bin/openmetadata_bootstrap.py" = file("${local.om_repo}/scripts/infrastructure/openmetadata_bootstrap.py")
    "bin/openmetadata_marquez.py"   = file("${local.om_repo}/scripts/infrastructure/openmetadata_marquez.py")
    "config/polaris.json"           = file("${local.om_repo}/config/metadata/ingestion/polaris.json")
    "config/dbt.json"               = file("${local.om_repo}/config/metadata/ingestion/dbt.json")
    "config/marquez.json"           = file("${local.om_repo}/config/metadata/ingestion/marquez.json")
  }
  om_init_script = join("\n", concat(
    [
      "set -eu",
      "mkdir -p /run/om /data/pg /data/os /workspace/bin /workspace/config /workspace/dbt",
      "echo $(($(date +%s) + OM_LIFETIME_SECONDS)) > /run/om/deadline",
      "head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \\n' > /run/om/db_password",
      "chmod 0444 /run/om/deadline /run/om/db_password",
    ],
    [for name, text in local.om_files : "printf '%s' '${base64gzip(text)}' | base64 -d | gunzip > /workspace/${name}"],
    [
      # Sticky and open, so the database user, the server user and the ingestion user can each create files here (the abort marker and the token) and none can remove another's.
      "chmod 1777 /run/om",
      "chown 999:999 /data/pg",
      "chown 1000:1000 /data/os",
      "chmod 1777 /tmp",
      "chown -R 10001:10001 /workspace",
    ]
  ))
  om_db_prelude = "DB_USER_PASSWORD=$(cat /run/om/db_password); export DB_USER_PASSWORD"
  om_server_environment = [
    { name = "DB_HOST", value = "127.0.0.1" },
    { name = "DB_PORT", value = "5432" },
    { name = "DB_SCHEME", value = "postgresql" },
    { name = "DB_DRIVER_CLASS", value = "org.postgresql.Driver" },
    { name = "DB_USER", value = "openmetadata_user" },
    { name = "OM_DATABASE", value = "openmetadata_db" },
    { name = "DB_USE_SSL", value = "false" },
    { name = "SEARCH_TYPE", value = "opensearch" },
    { name = "ELASTICSEARCH_HOST", value = "127.0.0.1" },
    { name = "ELASTICSEARCH_PORT", value = "9200" },
    { name = "ELASTICSEARCH_SCHEME", value = "http" },
    { name = "AUTHENTICATION_PROVIDER", value = "basic" },
    { name = "AUTHORIZER_ADMIN_PRINCIPALS", value = "[admin]" },
    { name = "AUTHORIZER_PRINCIPAL_DOMAIN", value = "open-metadata.org" },
    { name = "OPENMETADATA_HEAP_OPTS", value = "-Xmx6G -Xms6G" },
    # Without a pipeline service client the server cannot deploy or trigger ingestion pipelines. Nobody can start a profiler, sampling or auto-classification run from the UI.
    { name = "PIPELINE_SERVICE_CLIENT_ENABLED", value = "false" },
  ]
  om_common_environment = [
    { name = "AWS_REGION", value = var.aws_region },
    { name = "AWS_DEFAULT_REGION", value = var.aws_region },
    { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
    { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
    { name = "HOME", value = "/tmp" },
    { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
    { name = "PYTHONUNBUFFERED", value = "1" },
  ]
  om_python = "/workspace/bin"
  om_ingest = ["--config-dir", "/workspace/config", "--token-file", "/run/om/ingestion_token", "--work-dir", "/workspace"]
  om_steps = concat(
    [
      { name = "init", image = "database", user = "0:0", essential = false, writable = false, after = [], environment = [{ name = "OM_LIFETIME_SECONDS", value = tostring(var.openmetadata_lifetime_seconds) }], secrets = [], entry = ["sh", "-c", local.om_init_script], workdir = "/workspace" },
      {
        name        = "database", image = "database", user = "999:999", essential = true, writable = true, after = [["init", "SUCCESS"]], secrets = [], workdir = null
        environment = [{ name = "POSTGRES_USER", value = "openmetadata_user" }, { name = "POSTGRES_DB", value = "openmetadata_db" }, { name = "POSTGRES_PASSWORD_FILE", value = "/run/om/db_password" }, { name = "PGDATA", value = "/data/pg/data" }]
        entry       = concat(["sh", "-c", local.om_bounded, "openmetadata_bounded_run", tostring(local.om_extra)], var.openmetadata_commands.database)
      },
      {
        name        = "search", image = "search", user = "1000:1000", essential = true, writable = true, after = [["init", "SUCCESS"]], secrets = [], workdir = "/usr/share/opensearch"
        environment = [{ name = "discovery.type", value = "single-node" }, { name = "DISABLE_SECURITY_PLUGIN", value = "true" }, { name = "DISABLE_INSTALL_DEMO_CONFIG", value = "true" }, { name = "node.store.allow_mmap", value = "false" }, { name = "path.data", value = "/data/os" }, { name = "OPENSEARCH_JAVA_OPTS", value = "-Xms4g -Xmx4g" }]
        entry       = concat(["sh", "-c", local.om_bounded, "openmetadata_bounded_run", tostring(local.om_extra)], var.openmetadata_commands.search)
      },
      {
        name        = "migrate", image = "server", user = "1000:1000", essential = false, writable = true, after = [["init", "SUCCESS"]], secrets = [], workdir = null
        environment = local.om_server_environment
        entry       = concat(["sh", "-c", "${local.om_db_prelude}\n${local.om_migrate}", "openmetadata_migrate"], var.openmetadata_commands.migrate)
      },
      {
        name        = "server", image = "server", user = "1000:1000", essential = true, writable = true, after = [["migrate", "SUCCESS"]], secrets = [], workdir = null
        environment = local.om_server_environment
        entry       = concat(["sh", "-c", "${local.om_db_prelude}\n${local.om_bounded}", "openmetadata_bounded_run", "0"], var.openmetadata_commands.server)
      },
      {
        name        = "bootstrap", image = "ingestion", user = "10001:10001", essential = false, writable = false, after = [["server", "START"]], workdir = "/workspace"
        environment = []
        secrets     = [{ name = "OM_ADMIN_PASSWORD", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.openmetadata_admin_password_name}" }]
        entry       = ["python", "${local.om_python}/openmetadata_bootstrap.py"]
      },
      {
        name        = "ingest_polaris", image = "ingestion", user = "10001:10001", essential = false, writable = false, after = [["bootstrap", "SUCCESS"]], workdir = "/workspace"
        environment = [{ name = "RUN_ID", value = "" }, { name = "POLARIS_URI", value = "" }]
        secrets     = local.reader_parameters
        entry       = concat(["python", "${local.om_python}/openmetadata_ingest.py", "polaris"], local.om_ingest)
      },
      {
        name        = "ingest_dbt", image = "ingestion", user = "10001:10001", essential = false, writable = false, after = [["ingest_polaris", "COMPLETE"]], workdir = "/workspace"
        environment = [{ name = "RUN_ID", value = "" }]
        secrets     = []
        entry       = concat(["python", "${local.om_python}/openmetadata_ingest.py", "dbt"], local.om_ingest)
      },
    ],
    var.openmetadata_marquez_url == "" ? [] : [
      {
        name        = "ingest_marquez", image = "ingestion", user = "10001:10001", essential = false, writable = false, after = [["ingest_dbt", "COMPLETE"]], workdir = "/workspace"
        environment = [{ name = "RUN_ID", value = "" }]
        secrets     = []
        entry       = ["python", "${local.om_python}/openmetadata_marquez.py", "--marquez", var.openmetadata_marquez_url, "--config", "/workspace/config/marquez.json"]
      },
    ]
  )
  om_init_drop = ["DAC_OVERRIDE", "FSETID", "FOWNER", "MKNOD", "NET_RAW", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL", "AUDIT_WRITE"]
  om_containers = [
    for step in local.om_steps : merge(
      {
        name                   = step.name
        image                  = lookup(var.openmetadata_images, step.image, "invalid")
        essential              = step.essential
        privileged             = false
        readonlyRootFilesystem = !step.writable
        user                   = step.user
        entryPoint             = step.entry
        command                = []
        stopTimeout            = 60
        linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.om_init_drop : ["ALL"] }, initProcessEnabled = true }
        ulimits                = step.name == "search" ? [{ name = "core", softLimit = 0, hardLimit = 0 }, { name = "nofile", softLimit = 65536, hardLimit = 65536 }] : [{ name = "core", softLimit = 0, hardLimit = 0 }]
        dependsOn              = [for pair in step.after : { containerName = pair[0], condition = pair[1] }]
        environment            = concat(local.om_common_environment, step.environment)
        secrets                = step.secrets
        mountPoints = [
          { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
          { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false },
          { sourceVolume = "omrun", containerPath = "/run/om", readOnly = false },
          { sourceVolume = "pgdata", containerPath = "/data/pg", readOnly = false },
          { sourceVolume = "osdata", containerPath = "/data/os", readOnly = false },
        ]
        logConfiguration = {
          logDriver = "awslogs"
          options = {
            awslogs-group         = var.log_group_name
            awslogs-region        = var.aws_region
            awslogs-stream-prefix = "metadata"
            mode                  = "non-blocking"
            max-buffer-size       = "16m"
          }
        }
      },
      step.workdir == null ? {} : { workingDirectory = step.workdir }
    )
    if var.openmetadata_enabled
  ]
  # List prices per vCPU hour and GiB hour in us-west-2, from the owner's stated range of about $0.21 to $0.27 an hour at 4 vCPU and 24 GiB. Unverified here.
  om_hourly = {
    x86 = floor(((var.openmetadata_resources.cpu / 1024) * 0.04048 + (var.openmetadata_resources.memory / 1024) * 0.004445) * 100 + 0.5) / 100
    arm = floor(((var.openmetadata_resources.cpu / 1024) * 0.03238 + (var.openmetadata_resources.memory / 1024) * 0.00356) * 100 + 0.5) / 100
  }
}

resource "aws_ecs_task_definition" "openmetadata" {
  count = var.openmetadata_enabled ? 1 : 0

  family                   = "${var.project_name}_${var.environment}_job_metadata"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.openmetadata_resources.cpu)
  memory                   = tostring(var.openmetadata_resources.memory)
  execution_role_arn       = lookup(var.openmetadata_roles, "execution", "")
  task_role_arn            = lookup(var.openmetadata_roles, "openmetadata_task", "")
  container_definitions    = jsonencode(local.om_containers)
  skip_destroy             = true
  enable_fault_injection   = false
  tags                     = local.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.openmetadata_cpu_architecture
  }

  dynamic "volume" {
    for_each = ["workspace", "tmp", "omrun", "pgdata", "osdata"]
    content {
      name                = volume.value
      configure_at_launch = false
    }
  }

  dynamic "ephemeral_storage" {
    for_each = var.openmetadata_resources.ephemeral_storage_gib > 20 ? [var.openmetadata_resources.ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

module "openmetadata_flow" {
  source      = "../services/tests/flow"
  definitions = { for task in aws_ecs_task_definition.openmetadata : "metadata" => task.container_definitions }
  tasks = {
    for task in aws_ecs_task_definition.openmetadata : "metadata" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "openmetadata_container_definitions" {
  description = "Complete container JSON of the catalog task, or null while openmetadata_enabled is false."
  value       = one([for task in aws_ecs_task_definition.openmetadata : task.container_definitions])
}

output "openmetadata_secret_flow" {
  description = "Secret-flow and pin mutation results of the catalog task for review and mocked plan assertions."
  value = {
    violations          = module.openmetadata_flow.violations
    containers_checked  = module.openmetadata_flow.containers_checked
    mutation_mismatches = module.openmetadata_flow.mutation_mismatches
  }
}

output "openmetadata_run_task" {
  description = "What a run-task call for the catalog task needs, or null while openmetadata_enabled is false. Registration starts nothing."
  value = var.openmetadata_enabled ? {
    task_definition         = aws_ecs_task_definition.openmetadata[0].family
    assign_public_ip        = "DISABLED"
    capacity_providers      = ["FARGATE"]
    propagate_tags          = "TASK_DEFINITION"
    network_source          = "lakehouse group of the job_network output of the IAM stack"
    ui_port                 = 8585
    lifetime_seconds        = var.openmetadata_lifetime_seconds
    broker_deadline_seconds = var.openmetadata_lifetime_seconds + 900
    hourly_usd_range        = local.om_hourly
    container_environment   = { ingest_polaris = ["RUN_ID", "POLARIS_URI"], ingest_dbt = ["RUN_ID"] }
  } : null
}
