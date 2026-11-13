# The six model sets of the features job are host families of their own (features_m1 to features_m6): the same containers, roles and variables as the features
# job, but each has its own host task definition, launch records and scratch folder, so the launch library's family gate lets the six run beside each other,
# and each reserves its own size at task level instead of the whole job room. Drift, promote, serve, metadata and the publisher keep their own sizes the same
# way (rows with reserve true in the host table), so different tests share the host. Silver, gold, the features base build, bronze and train reserve the whole room.
locals {
  # The host table of the launch library: what each family reserves. A model set (reserve true) reserves its row's cpu_units and memory_mib, so the
  # launcher's fit check, the pool weights and the task definition read the same numbers. A case of scripts/orchestration/run_airflow_mode_e2e.py checks
  # the rows; the measured peaks behind the sizes are in the file's reserve_note.
  host_table      = jsondecode(file("${path.module}/../../config/orchestration/host_jobs.json"))
  host_model_sets = ["m1", "m2", "m3", "m4", "m5", "m6"]
  # Each set family and the family whose containers, roles and variables it uses.
  host_set_families = { for set in local.host_model_sets : "features_${set}" => "features" }
  # What each selected family reserves at task level: the row's own cpu_units and memory_mib when the row says reserve true (a model set, drift, promote,
  # serve and metadata), else the whole job room (host_cpu_cap and host_memory_cap).
  host_family_reserves = { for family in keys(local.host_selected) : family => try(local.host_table.jobs[family].reserve, false) }
  host_family_cpu      = { for family, own in local.host_family_reserves : family => own ? local.host_table.jobs[family].cpu_units : local.host_cpu_cap }
  host_family_memory   = { for family, own in local.host_family_reserves : family => own ? local.host_table.jobs[family].memory_mib : local.host_memory_cap }
  # The capacity binding of each family's revision: the services output with the family's own job memory, so the preflight finds the memory the revision
  # reserves (a small family's revision is bound to its own memory, not to the cap) and the same usable memory, reserve and service reservations.
  host_family_binding = {
    for family, own in local.host_family_reserves : family => (
      own && var.host_capacity_binding != null
      ? replace(var.host_capacity_binding, "/^job_memory_mib=[0-9]+/", "job_memory_mib=${local.host_family_memory[family]}")
      : coalesce(var.host_capacity_binding, "unset")
    )
  }
  # The publisher host definition (publisher.tf) reads its row the same way.
  publisher_reserves = try(local.host_table.jobs.publisher.reserve, false)
  publisher_cpu      = local.publisher_reserves ? local.host_table.jobs.publisher.cpu_units : local.host_cpu_cap
  publisher_memory   = local.publisher_reserves ? local.host_table.jobs.publisher.memory_mib : local.host_memory_cap
  publisher_binding = (
    local.publisher_reserves && var.host_capacity_binding != null
    ? replace(var.host_capacity_binding, "/^job_memory_mib=[0-9]+/", "job_memory_mib=${local.publisher_memory}")
    : coalesce(var.host_capacity_binding, "unset")
  )
  # The container variables and dry run override of a set are those of its base family.
  host_run_variables_of = merge(local.host_run_variables, { for family, base in local.host_set_families : family => local.host_run_variables[base] })
  host_dry_run_of       = merge(local.host_dry_run, { for family, base in local.host_set_families : family => local.host_dry_run[base] })
}
