#!/usr/bin/env bash
# Prints the build arguments of one image of the registry file, one per line: the Dockerfile (--file=, only when the entry names one), each named build
# context in key order, then the build context as the last line. The Dockerfile path is relative to the repository root and follows
# scripts/infrastructure/push_images.py: the entry's dockerfile field is a path inside its context. An entry without it builds the Dockerfile of the context.
# Usage: bash .github/scripts/image_build_args.sh <images.json> <image>
set -euo pipefail

registry="${1:?the registry file}"
image="${2:?the image name}"

jq -e --arg name "$image" '.images[$name] | select(.kind == "build")' "$registry" > /dev/null || {
  echo "Only an image of kind build has build arguments: $image." >&2
  exit 1
}
jq -r --arg name "$image" '
  .images[$name] as $entry
  | ($entry.context) as $context
  | (if $entry.dockerfile == null then empty
     else "--file=" + (if $context == "." then $entry.dockerfile else ($context | rtrimstr("/")) + "/" + $entry.dockerfile end) end),
    ($entry.build_contexts // {} | to_entries | sort_by(.key)[] | "--build-context=\(.key)=\(.value)"),
    $context' "$registry"
