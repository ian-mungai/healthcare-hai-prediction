output "violations" {
  description = "Every script line through which an injected secret, or a variable that received it, reaches a file, a command argument, the log or the process environment dump."
  value       = local.violations
}

output "containers_checked" {
  description = "The containers that receive at least one secret and were followed."
  value       = sort(keys(local.with_secrets))
}

output "alias_chains_converged" {
  description = "True when one more assignment pass finds no new variable, so no alias chain is longer than the passes followed."
  value       = local.converged
}

output "execution_digests" {
  description = "SHA-256 of each container's complete definition (less the named exclusions) and the task settings its execution depends on, to compare with the approved versions."
  value       = local.execution_digests
}

output "mutation_mismatches" {
  description = "Mutations whose effect on the digest differs from the expectation: a bound field or referenced volume that left the digest unchanged, or an excluded field that changed it."
  value       = local.mutation_mismatches
}

output "mutations_checked" {
  description = "Number of mutated variants compared with the base digest."
  value       = length(local.mutated)
}

output "excluded_fields_exercised" {
  description = "Excluded container fields for which at least one mutation was applied and left the digest unchanged."
  value       = sort(distinct([for variant in local.mutated : variant.field if variant.part == "container" && !variant.changed && !variant.expected]))
}

output "mutation_paths" {
  description = "For each container, every path that a mutation replaced: fields as a whole, then keys and elements inside them, then keys inside those."
  value = {
    for id, subject in local.subjects : id => sort([for variant in local.mutated : variant.path if variant.id == id])
  }
}
