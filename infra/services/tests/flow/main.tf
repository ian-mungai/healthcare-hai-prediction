# Follows each injected secret through later assignments, printf, echo, redirections and here-documents in the
# command lines of a container, without running anything.
locals {
  name = "[A-Za-z_][A-Za-z0-9_]*"

  assign_re  = "^(?:(?:export|local|readonly|declare)\\s+(?:-[A-Za-z]+\\s+)?)?(${local.name})=(.*)$"
  printfv_re = "^printf\\s+-v\\s+(${local.name})\\s(.*)$"
  set_re     = "^\\\\set\\s+(${local.name})\\s+(.*)$"
  getenv_re  = "^\\\\getenv\\s+(${local.name})\\s+(${local.name})\\s*$"
  sources    = [local.assign_re, local.printfv_re, local.set_re]

  single_word_re   = "^(?:\\\\.|[^\\s\"'\\\\]|\"[^\"]*\"|'[^']*')*$"
  substitution_re  = "^\"?\\$\\((printf|echo) [^<>|;&`()]*\\)\"?$"
  unsafe_syntax_re = "[<>|;&`]|\\$\\(|\\\\$"
  file_write_re    = "(^|[^<>&])>>?(\\s*[^&\\s>]|\\s*$)|&>"
  literal_file_re  = "^cat\\s+>\\s*\\S+\\s+<<-?\\s*'[A-Za-z_]+'\\s*$"
  unquoted_doc_re  = "<<-?\\s*[A-Za-z_]"
  tracing_re       = "(^|\\s)(set\\s+(-[a-z]*x|-o\\s+(xtrace|history))|bash\\s+-[a-z]*x)|^-[a-z]*x[a-z]*$"
  env_dump_re      = "(^|[^A-Za-z0-9_.-])(env|printenv|compgen -e)($|[^A-Za-z0-9_=.-])|(^|[;&|(]\\s*)set\\s*($|[;&|)])|(export|declare|typeset)\\s+-[a-z]*p|/proc/[^ ]*environ"
  reflective_re    = "(^|[^A-Za-z0-9_])(eval|source|trap|shopt|enable|builtin)(\\s|$)|(^|[^A-Za-z0-9_])set\\s+[-+][A-Za-z]*[ovx]|BASH_XTRACEFD|BASH_ENV|(^|[^A-Za-z0-9_])PS4=|\\$\\{![A-Za-z_]|(declare|typeset|local)\\s+-[A-Za-z]*n|\\$\\{[A-Za-z_][A-Za-z0-9_]*@[A-Za-z]\\}"
  opener_re        = "<\\(\\s*$"
  closer_re        = "^\\)"
  sql_password_re  = "^(?i:(ALTER|CREATE)\\s+ROLE\\s+\\w+\\s+.*PASSWORD\\s+:'[a-z_]+')\\s*;"
  sql_unsafe_re    = "(?i)\\\\|copy|lo_export|pg_file_write|outfile|dumpfile"
  stdin_consumers  = "^exec (redis-server -|timeout [0-9]+ /opt/jboss/container/java/run/run-java\\.sh bootstrap --credentials-file /dev/stdin) < <\\($"

  containers = flatten([
    for key, json in var.definitions : [
      for container in jsondecode(json) : {
        id      = "${key}/${container.name}"
        secrets = [for secret in lookup(container, "secrets", []) : secret.name]
        lines = compact([
          for line in flatten([
            for element in concat(lookup(container, "entryPoint", []), lookup(container, "command", []), lookup(lookup(container, "healthCheck", {}), "command", [])) : split("\n", element)
          ]) : trimspace(line)
        ])
      }
    ]
  ])

  with_secrets = { for container in local.containers : container.id => container if length(container.secrets) > 0 }

  pass0 = { for id, container in local.with_secrets : id => container.secrets }
  ref0  = { for id, names in local.pass0 : id => "\\$\\{?[!#]?(?:${join("|", names)})([^A-Za-z0-9_]|$)|:['\"]?(?:${join("|", names)})\\b|(?:environ|getenv)\\W+(?:\\w+\\W+)?(?:${join("|", names)})(?:\\W|$)" }

  pass1 = { for id, container in local.with_secrets : id => distinct(concat(local.pass0[id],
    flatten([for line in container.lines : [for re in local.sources : regex(re, line)[0] if can(regex(re, line)) && can(regex(local.ref0[id], regex(re, line)[1]))]]),
    [for line in container.lines : regex(local.getenv_re, line)[0] if can(regex(local.getenv_re, line)) && contains(local.pass0[id], regex(local.getenv_re, line)[1])],
  )) }
  ref1 = { for id, names in local.pass1 : id => "\\$\\{?[!#]?(?:${join("|", names)})([^A-Za-z0-9_]|$)|:['\"]?(?:${join("|", names)})\\b|(?:environ|getenv)\\W+(?:\\w+\\W+)?(?:${join("|", names)})(?:\\W|$)" }

  pass2 = { for id, container in local.with_secrets : id => distinct(concat(local.pass1[id],
    flatten([for line in container.lines : [for re in local.sources : regex(re, line)[0] if can(regex(re, line)) && can(regex(local.ref1[id], regex(re, line)[1]))]]),
    [for line in container.lines : regex(local.getenv_re, line)[0] if can(regex(local.getenv_re, line)) && contains(local.pass1[id], regex(local.getenv_re, line)[1])],
  )) }
  ref2 = { for id, names in local.pass2 : id => "\\$\\{?[!#]?(?:${join("|", names)})([^A-Za-z0-9_]|$)|:['\"]?(?:${join("|", names)})\\b|(?:environ|getenv)\\W+(?:\\w+\\W+)?(?:${join("|", names)})(?:\\W|$)" }

  pass3 = { for id, container in local.with_secrets : id => distinct(concat(local.pass2[id],
    flatten([for line in container.lines : [for re in local.sources : regex(re, line)[0] if can(regex(re, line)) && can(regex(local.ref2[id], regex(re, line)[1]))]]),
    [for line in container.lines : regex(local.getenv_re, line)[0] if can(regex(local.getenv_re, line)) && contains(local.pass2[id], regex(local.getenv_re, line)[1])],
  )) }
  ref3 = { for id, names in local.pass3 : id => "\\$\\{?[!#]?(?:${join("|", names)})([^A-Za-z0-9_]|$)|:['\"]?(?:${join("|", names)})\\b|(?:environ|getenv)\\W+(?:\\w+\\W+)?(?:${join("|", names)})(?:\\W|$)" }

  pass4 = { for id, container in local.with_secrets : id => distinct(concat(local.pass3[id],
    flatten([for line in container.lines : [for re in local.sources : regex(re, line)[0] if can(regex(re, line)) && can(regex(local.ref3[id], regex(re, line)[1]))]]),
    [for line in container.lines : regex(local.getenv_re, line)[0] if can(regex(local.getenv_re, line)) && contains(local.pass3[id], regex(local.getenv_re, line)[1])],
  )) }
  ref4 = { for id, names in local.pass4 : id => "\\$\\{?[!#]?(?:${join("|", names)})([^A-Za-z0-9_]|$)|:['\"]?(?:${join("|", names)})\\b|(?:environ|getenv)\\W+(?:\\w+\\W+)?(?:${join("|", names)})(?:\\W|$)" }

  pass5 = { for id, container in local.with_secrets : id => distinct(concat(local.pass4[id],
    flatten([for line in container.lines : [for re in local.sources : regex(re, line)[0] if can(regex(re, line)) && can(regex(local.ref4[id], regex(re, line)[1]))]]),
    [for line in container.lines : regex(local.getenv_re, line)[0] if can(regex(local.getenv_re, line)) && contains(local.pass4[id], regex(local.getenv_re, line)[1])],
  )) }
  ref5 = { for id, names in local.pass5 : id => "\\$\\{?[!#]?(?:${join("|", names)})([^A-Za-z0-9_]|$)|:['\"]?(?:${join("|", names)})\\b|(?:environ|getenv)\\W+(?:\\w+\\W+)?(?:${join("|", names)})(?:\\W|$)" }

  converged = alltrue([for id, names in local.pass5 : length(names) == length(local.pass4[id])])

  checked = { for id, container in local.with_secrets : id => [
    for index, line in container.lines : {
      line        = line
      holds       = can(regex(local.ref5[id], line))
      inside      = length([for before, text in container.lines : text if before < index && can(regex(local.opener_re, text))]) > length([for before, text in container.lines : text if before < index && can(regex(local.closer_re, text))])
      safe_syntax = !can(regex(local.unsafe_syntax_re, line))
      assignment = (
        can(regex(local.assign_re, line)) &&
        !can(regex("[<>|;&`]|\\$\\(", regex(local.assign_re, line)[1])) &&
        (can(regex(local.single_word_re, regex(local.assign_re, line)[1])) || can(regex(local.substitution_re, regex(local.assign_re, line)[1])))
      )
      printf_to_variable = can(regex(local.printfv_re, line)) && !can(regex(local.unsafe_syntax_re, line))
      getenv             = can(regex(local.getenv_re, line))
      psql_set           = can(regex("^\\\\set\\s", line)) && !can(regex(local.unsafe_syntax_re, line))
      sql_password       = can(regex(local.sql_password_re, line)) && !can(regex(local.sql_unsafe_re, line))
      placeholder        = can(regex("^[A-Za-z_][A-Za-z0-9_]*:\\s*\"?\\$\\{${local.name}\\}\"?$", line))
      printf_text        = can(regex("^(printf|echo)\\s", line))
    }
  ] }

  violations = flatten([
    for id, container in local.with_secrets : concat(
      [
        for entry in local.checked[id] : "${id}: ${entry.line}"
        if entry.holds && !(
          entry.assignment || entry.printf_to_variable || entry.getenv || entry.psql_set || entry.sql_password || entry.placeholder ||
          (entry.printf_text && entry.safe_syntax && entry.inside)
        )
      ],
      [for line in container.lines : "${id}: ${line}" if can(regex(local.file_write_re, line)) && !can(regex(local.literal_file_re, line))],
      [for line in container.lines : "${id}: ${line}" if can(regex(local.unquoted_doc_re, line)) || can(regex(local.tracing_re, line)) || can(regex(local.env_dump_re, line))],
      [for line in container.lines : "${id}: ${line}" if can(regex(local.opener_re, line)) && !can(regex(local.stdin_consumers, line))],
      [for line in container.lines : "${id}: ${line}" if can(regex(local.reflective_re, line))],
    )
  ])
}

locals {
  # The approval pin binds the complete definition of each container and the task settings its execution depends on, minus the
  # named exclusions below. Nothing is picked by name: a field that is added later is bound until someone excludes it here with
  # a reason. Values are bound nested and in order, so a changed indent in a here-document, a script split into separate
  # arguments, a tracing variable (literal, injected as a secret binding or set by an environment file), a changed mount target
  # or a working directory all change the digest.
  excluded_container_fields = {
    memory            = "Hard memory limit. Sizing only: the memory tests bound it against the host and the reservation, and it changes no command, file, variable, secret or network setting."
    memoryReservation = "Soft memory reservation. Sizing only, bounded by the same tests."
  }

  # Task settings: every attribute the root module reports (see task_definition_settings) except container_definitions, which the
  # container pins bind one container at a time. Of the task volumes, a container is bound to the ones its mount points name; a
  # volume it does not mount cannot change what it runs and is the second named exclusion.
  marker = "__mutated__"
  parts  = ["container", "task"]

  subjects = merge([
    for key, json in var.definitions : {
      for container in jsondecode(json) : "${key}/${container.name}" => {
        container = container
        task      = var.tasks[key]
      }
    }
  ]...)

  referenced_volume_indexes = {
    for id, subject in local.subjects : id => [
      for index, volume in subject.task.volume : index
      if contains(try([for mount in subject.container.mountPoints : mount.sourceVolume], []), volume.name)
    ]
  }

  # One variant per field of each part, per key or element of an object or list value, and per key of an object inside those.
  # Each replaces one value with the marker. The base variant has the empty path.
  level_field = flatten([
    for id, subject in local.subjects : [
      for part in local.parts : [
        for field, value in subject[part] : {
          id      = id, part = part, field = field, index = null, path = "${part}.${field}"
          subject = merge(subject, { (part) = merge(subject[part], { (field) = local.marker }) })
        }
      ]
    ]
  ])

  level_key = flatten([
    for id, subject in local.subjects : [
      for part in local.parts : [
        for field, value in subject[part] : [
          for key in(can(keys(value)) ? keys(value) : []) : {
            id      = id, part = part, field = field, index = null, path = "${part}.${field}.${key}"
            subject = merge(subject, { (part) = merge(subject[part], { (field) = merge(value, { (key) = local.marker }) }) })
          }
        ]
      ]
    ]
  ])

  level_element = flatten([
    for id, subject in local.subjects : [
      for part in local.parts : [
        for field, value in subject[part] : [
          for index in(can(value[0]) ? range(length(value)) : []) : {
            id      = id, part = part, field = field, index = index, path = "${part}.${field}[${index}]"
            subject = merge(subject, { (part) = merge(subject[part], { (field) = concat(slice(value, 0, index), [local.marker], slice(value, index + 1, length(value))) }) })
          }
        ]
      ]
    ]
  ])

  level_key_in_key = flatten([
    for id, subject in local.subjects : [
      for part in local.parts : [
        for field, value in subject[part] : [
          for key in(can(keys(value)) ? keys(value) : []) : [
            for inner in(can(keys(value[key])) ? keys(value[key]) : []) : {
              id      = id, part = part, field = field, index = null, path = "${part}.${field}.${key}.${inner}"
              subject = merge(subject, { (part) = merge(subject[part], { (field) = merge(value, { (key) = merge(value[key], { (inner) = local.marker }) }) }) })
            }
          ]
        ]
      ]
    ]
  ])

  level_element_in_key = flatten([
    for id, subject in local.subjects : [
      for part in local.parts : [
        for field, value in subject[part] : [
          for key in(can(keys(value)) ? keys(value) : []) : [
            for index in(can(value[key][0]) ? range(length(value[key])) : []) : {
              id      = id, part = part, field = field, index = null, path = "${part}.${field}.${key}[${index}]"
              subject = merge(subject, { (part) = merge(subject[part], { (field) = merge(value, { (key) = concat(slice(value[key], 0, index), [local.marker], slice(value[key], index + 1, length(value[key]))) }) }) })
            }
          ]
        ]
      ]
    ]
  ])

  level_key_in_element = flatten([
    for id, subject in local.subjects : [
      for part in local.parts : [
        for field, value in subject[part] : [
          for index in(can(value[0]) ? range(length(value)) : []) : [
            for inner in(can(keys(value[index])) ? keys(value[index]) : []) : {
              id      = id, part = part, field = field, index = index, path = "${part}.${field}[${index}].${inner}"
              subject = merge(subject, { (part) = merge(subject[part], { (field) = concat(slice(value, 0, index), [merge(value[index], { (inner) = local.marker })], slice(value, index + 1, length(value))) }) })
            }
          ]
        ]
      ]
    ]
  ])

  variants = concat(
    [for id, subject in local.subjects : { id = id, part = "", field = "", index = null, path = "", subject = subject }],
    local.level_field, local.level_key, local.level_element, local.level_key_in_key, local.level_element_in_key, local.level_key_in_element,
  )

  digested = [
    for variant in local.variants : merge(variant, {
      digest = sha256(jsonencode({
        container = { for field, value in variant.subject.container : field => value if !contains(keys(local.excluded_container_fields), field) }
        task = {
          settings = { for attribute, value in variant.subject.task : attribute => value if attribute != "volume" }
          volumes = {
            for volume in try([for volume in variant.subject.task.volume : volume], []) : try(volume.name, "") => volume
            if contains(try([for mount in variant.subject.container.mountPoints : mount.sourceVolume], []), try(volume.name, ""))
          }
        }
      }))
    })
  ]

  execution_digests = { for variant in local.digested : variant.id => variant.digest if variant.path == "" }

  # What a mutation must do, derived from the exclusions and the container's mounts, not from the digest code.
  mutated = [
    for variant in local.digested : merge(variant, {
      changed = variant.digest != local.execution_digests[variant.id]
      expected = (
        variant.part == "container" ? !contains(keys(local.excluded_container_fields), variant.field)
        : variant.field != "volume" ? true
        : variant.index == null ? length(local.referenced_volume_indexes[variant.id]) > 0
        : contains(local.referenced_volume_indexes[variant.id], variant.index)
      )
    }) if variant.path != ""
  ]

  mutation_mismatches = [for variant in local.mutated : "${variant.id}: ${variant.path} changed=${variant.changed} expected=${variant.expected}" if variant.changed != variant.expected]
}
