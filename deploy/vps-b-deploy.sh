#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: vps-b-deploy.sh DEPLOY_DIR COMMIT IMAGE_REPOSITORY IMAGE_ARCHIVE IMAGE_SHA256 COMPOSE_FILE" >&2
  exit 2
fi

deploy_dir=$1
commit=$2
image_repository=$3
image_archive=$4
checksum_source=$5
compose_source=$6
incoming_archive=$image_archive
incoming_checksum=$checksum_source
incoming_compose=$compose_source
trap 'rm -f -- "$incoming_archive" "$incoming_checksum" "$incoming_compose"' EXIT

if [[ ! $commit =~ ^[0-9a-f]{40}$ ]]; then
  echo "invalid commit SHA" >&2
  exit 2
fi
if [[ ! $image_repository =~ ^[a-z0-9][a-z0-9._/-]*$ ]]; then
  echo "invalid image repository" >&2
  exit 2
fi
if [[ $deploy_dir != /opt/ptt-alertor ]]; then
  echo "unexpected deployment directory" >&2
  exit 2
fi

env_file="$deploy_dir/.env"
if [[ ! -f $env_file ]]; then
  echo "$env_file is required and is never supplied by CI" >&2
  exit 1
fi
env_mode=$(stat -c '%a' "$env_file")
if (( (8#$env_mode & 077) != 0 )); then
  echo "$env_file must not be readable or writable by group/other" >&2
  exit 1
fi
if [[ ! -s $image_archive || ! -s $checksum_source || ! -s $compose_source ]]; then
  echo "deployment artifact is missing" >&2
  exit 1
fi
expected_checksum=$(tr -d '[:space:]' <"$checksum_source")
if [[ ! $expected_checksum =~ ^[0-9a-f]{64}$ ]]; then
  echo "image checksum file is invalid" >&2
  exit 1
fi
actual_checksum=$(sha256sum "$image_archive" | awk '{print $1}')
if [[ $actual_checksum != "$expected_checksum" ]]; then
  echo "image checksum verification failed" >&2
  exit 1
fi

state_dir="$deploy_dir/.deploy"
release_dir="$state_dir/releases/$commit"
new_image="$image_repository:$commit"

install -d -m 700 "$state_dir" "$state_dir/releases"

current_healthy() {
  curl --fail --silent --show-error --max-time 5 http://127.0.0.1:9090/healthz >/dev/null \
    && curl --fail --silent --show-error --max-time 5 http://127.0.0.1:9090/readyz >/dev/null
}

# The first workflow deployment may take over an app that was staged manually
# before release markers existed. Preserve that healthy image and compose file
# so the first deployment has the same rollback guarantee as later ones.
bootstrap_current_release() {
  local app_container current_image_id bootstrap_commit bootstrap_image bootstrap_release next_link
  if [[ -L $state_dir/current || ! -f $deploy_dir/compose.yaml ]]; then
    return
  fi
  app_container=$(docker compose \
    --project-name ptt-alertor \
    --project-directory "$deploy_dir" \
    --env-file "$env_file" \
    -f "$deploy_dir/compose.yaml" ps -q app 2>/dev/null || true)
  if [[ -z $app_container ]]; then
    return
  fi
  if ! current_healthy; then
    echo "existing app is not healthy; refusing to replace it without a rollback target" >&2
    exit 1
  fi
  current_image_id=$(docker inspect --format '{{.Image}}' "$app_container")
  if [[ ! $current_image_id =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "existing app image ID is invalid" >&2
    exit 1
  fi
  bootstrap_commit=${current_image_id#sha256:}
  bootstrap_commit=${bootstrap_commit:0:40}
  if [[ $bootstrap_commit == "$commit" ]]; then
    echo "existing image marker conflicts with deployment commit" >&2
    exit 1
  fi
  bootstrap_image="$image_repository:$bootstrap_commit"
  bootstrap_release="$state_dir/releases/$bootstrap_commit"
  install -d -m 700 "$bootstrap_release"
  install -m 644 "$deploy_dir/compose.yaml" "$bootstrap_release/compose.yaml"
  docker image tag "$current_image_id" "$bootstrap_image"
  printf '%s\n' "$bootstrap_image" >"$bootstrap_release/image"
  chmod 600 "$bootstrap_release/image"
  next_link="$state_dir/current.next"
  rm -f -- "$next_link"
  ln -s "releases/$bootstrap_commit" "$next_link"
  mv -Tf "$next_link" "$state_dir/current"
}

bootstrap_current_release

install -d -m 700 "$release_dir"
install -m 644 "$compose_source" "$release_dir/compose.yaml"
printf '%s\n' "$new_image" >"$release_dir/image"
chmod 600 "$release_dir/image"

previous_release=""
previous_image=""
if [[ -L $state_dir/current ]]; then
  previous_release=$(readlink -f "$state_dir/current" || true)
  if [[ $previous_release != "$state_dir/releases/"* || ! -f $previous_release/compose.yaml || ! -f $previous_release/image ]]; then
    echo "current release marker is invalid" >&2
    exit 1
  fi
  previous_image=$(<"$previous_release/image")
  if [[ ! $previous_image =~ ^[a-z0-9][a-z0-9._/-]*:[0-9a-f]{40}$ || ${previous_image%:*} != "$image_repository" ]]; then
    echo "previous image marker is invalid" >&2
    exit 1
  fi
fi

gzip -dc "$image_archive" | docker load >/dev/null
if ! docker image inspect "$new_image" >/dev/null 2>&1; then
  echo "loaded image tag does not match expected commit" >&2
  exit 1
fi

compose_for() {
  local release=$1
  local image=$2
  shift 2
  APP_IMAGE="$image" docker compose \
    --project-name ptt-alertor \
    --project-directory "$deploy_dir" \
    --env-file "$env_file" \
    -f "$release/compose.yaml" "$@"
}

healthy() {
  local attempt
  for attempt in {1..24}; do
    if current_healthy; then
      return 0
    fi
    sleep 5
  done
  return 1
}

remove_release() {
  local candidate_release=$1
  local candidate_image=""
  if [[ $candidate_release == "$release_dir" || $candidate_release == "$previous_release" ]]; then
    return
  fi
  if [[ -f $candidate_release/image ]]; then
    candidate_image=$(<"$candidate_release/image")
    if [[ $candidate_image =~ ^[a-z0-9][a-z0-9._/-]*:[0-9a-f]{40}$ && ${candidate_image%:*} == "$image_repository" ]]; then
      docker image rm "$candidate_image" >/dev/null 2>&1 || true
    fi
  fi
  rm -f -- "$candidate_release/compose.yaml" "$candidate_release/image"
  rmdir -- "$candidate_release" 2>/dev/null || true
}

cleanup_old_images() {
  local candidate candidate_name
  shopt -s nullglob
  for candidate in "$state_dir/releases/"*; do
    [[ -d $candidate ]] || continue
    candidate_name=${candidate##*/}
    [[ $candidate_name =~ ^[0-9a-f]{40}$ ]] || continue
    remove_release "$candidate"
  done
  while IFS= read -r candidate; do
    [[ $candidate =~ ^[a-z0-9][a-z0-9._/-]*:[0-9a-f]{40}$ && ${candidate%:*} == "$image_repository" ]] || continue
    if [[ $candidate == "$new_image" || $candidate == "$previous_image" ]]; then
      continue
    fi
    docker image rm "$candidate" >/dev/null 2>&1 || true
  done < <(docker image ls "$image_repository" --format '{{.Repository}}:{{.Tag}}')
}

discard_failed_release() {
  if [[ $release_dir == "$previous_release" ]]; then
    return
  fi
  docker image rm "$new_image" >/dev/null 2>&1 || true
  rm -f -- "$release_dir/compose.yaml" "$release_dir/image"
  rmdir -- "$release_dir" 2>/dev/null || true
}

compose_for "$release_dir" "$new_image" config --quiet
compose_for "$release_dir" "$new_image" up -d --no-build redis
compose_for "$release_dir" "$new_image" up -d --no-build app

if healthy; then
  next_link="$state_dir/current.next"
  rm -f -- "$next_link"
  ln -s "releases/$commit" "$next_link"
  mv -Tf "$next_link" "$state_dir/current"
  cleanup_old_images
  echo "deployed $commit"
  exit 0
fi

echo "health checks failed for $commit; attempting rollback" >&2
if [[ -n $previous_release ]]; then
  compose_for "$previous_release" "$previous_image" config --quiet
  compose_for "$previous_release" "$previous_image" up -d --no-build redis
  compose_for "$previous_release" "$previous_image" up -d --no-build app
  if healthy; then
    echo "rolled back to ${previous_image##*:}" >&2
  else
    echo "rollback health checks also failed" >&2
  fi
else
  compose_for "$release_dir" "$new_image" stop app || true
  compose_for "$release_dir" "$new_image" rm -f app || true
  echo "no previous release existed; stopped the failed app" >&2
fi
discard_failed_release
exit 1
