#!/usr/bin/env bash
# Roll ECS services onto a new image, optionally running a one-off migration first.
#
#   ecs-deploy.sh --cluster cdp-prod-apps --repo cdp/connector-api --tag <sha> \
#     [--migrate "uv run alembic upgrade head"] service [service...]
#
# For each service: copy its current task definition, swap every container image
# from --repo to :<tag>, register a new revision and update the service. The
# migration (if any) runs with the first service's new revision before any
# service is updated. When every service is stable, :prod is moved to <tag>, so
# CDK-managed revisions (pinned to :prod) resolve to the same image.
# Canonical copy: buzz-cdp-workspace/ops/ci/ecs-deploy.sh
set -euo pipefail

cluster="" repo="" tag="" migrate=""
services=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster) cluster="$2"; shift 2 ;;
    --repo) repo="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --migrate) migrate="$2"; shift 2 ;;
    *) services+=("$1"); shift ;;
  esac
done
[[ -n "$cluster" && -n "$repo" && -n "$tag" && ${#services[@]} -gt 0 ]] || { echo "usage: see header" >&2; exit 2; }

account="$(aws sts get-caller-identity --query Account --output text)"
registry="${account}.dkr.ecr.${AWS_REGION}.amazonaws.com"
image="${registry}/${repo}:${tag}"
aws ecr describe-images --repository-name "$repo" --image-ids imageTag="$tag" >/dev/null

declare -A new_td
for svc in "${services[@]}"; do
  current="$(aws ecs describe-services --cluster "$cluster" --services "$svc" --query 'services[0].taskDefinition' --output text)"
  aws ecs describe-task-definition --task-definition "$current" --include TAGS --output json \
    | jq --arg repo "${registry}/${repo}:" --arg image "$image" '
        .tags as $tags | .taskDefinition
        | .containerDefinitions |= map(if (.image | startswith($repo)) then .image = $image else . end)
        | {family, taskRoleArn, executionRoleArn, networkMode, containerDefinitions, volumes,
           placementConstraints, requiresCompatibilities, cpu, memory, runtimePlatform, ephemeralStorage}
        | with_entries(select(.value != null))
        + (if ($tags | length) > 0 then {tags: $tags} else {} end)' > "/tmp/td-${svc}.json"
  new_td[$svc]="$(aws ecs register-task-definition --cli-input-json "file:///tmp/td-${svc}.json" --query 'taskDefinition.taskDefinitionArn' --output text)"
  echo "registered ${new_td[$svc]}"
done

if [[ -n "$migrate" ]]; then
  first="${services[0]}"
  netcfg="$(aws ecs describe-services --cluster "$cluster" --services "$first" --query 'services[0].networkConfiguration' --output json)"
  container="$(jq -r '.containerDefinitions[0].name' "/tmp/td-${first}.json")"
  overrides="$(jq -nc --arg c "$container" --arg cmd "$migrate" '{containerOverrides: [{name: $c, command: ["sh", "-c", $cmd]}]}')"
  task="$(aws ecs run-task --cluster "$cluster" --task-definition "${new_td[$first]}" --launch-type FARGATE \
    --network-configuration "$netcfg" --overrides "$overrides" --started-by "ci-migrate-${tag:0:12}" \
    --query 'tasks[0].taskArn' --output text)"
  echo "migration task $task"
  aws ecs wait tasks-stopped --cluster "$cluster" --tasks "$task"
  code="$(aws ecs describe-tasks --cluster "$cluster" --tasks "$task" --query 'tasks[0].containers[0].exitCode' --output text)"
  if [[ "$code" != "0" ]]; then
    echo "migration failed (exit $code); services not updated" >&2
    exit 1
  fi
fi

for svc in "${services[@]}"; do
  aws ecs update-service --cluster "$cluster" --service "$svc" --task-definition "${new_td[$svc]}" --query 'service.serviceName' --output text
done
echo "waiting for services to stabilize..."
aws ecs wait services-stable --cluster "$cluster" --services "${services[@]}"

for svc in "${services[@]}"; do
  # Services without a load balancer turn COMPLETED a little after services-stable.
  for _ in $(seq 1 60); do
    state="$(aws ecs describe-services --cluster "$cluster" --services "$svc" --query 'services[0].deployments[0].rolloutState' --output text)"
    [[ "$state" == "IN_PROGRESS" ]] || break
    sleep 10
  done
  [[ "$state" == "COMPLETED" ]] || { echo "$svc rollout state $state (circuit breaker rolled back?)" >&2; exit 1; }
done

manifest="$(aws ecr batch-get-image --repository-name "$repo" --image-ids imageTag="$tag" --query 'images[0].imageManifest' --output text)"
aws ecr put-image --repository-name "$repo" --image-tag prod --image-manifest "$manifest" >/dev/null 2>&1 \
  || echo "note: :prod already points at this image"
echo "deployed ${image} to: ${services[*]}"
