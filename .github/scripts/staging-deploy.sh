#!/usr/bin/env bash
# Deploy a Docker Compose project to the cdp-stg box through SSM Run Command.
#
#   staging-deploy.sh --dir /home/ec2-user/staging-apps/<app> --tag <image tag> \
#     [--compose compose.staging.yaml] [--healthy "api other"]
#
# Ships the repo's compose file to the box, writes REGISTRY/IMAGE_TAG into the project's
# .env (interpolation only; secrets stay in .env.staging on the box), pulls from ECR,
# `up -d` (migrate runs as a compose one-shot), then waits until the --healthy services
# report healthy. The box runs Mon-Fri 08:00-20:00 Asia/Bangkok; outside that it fails
# fast with the command that starts it.
set -euo pipefail

instance="${STAGING_INSTANCE_ID:-i-05a9fa5b081f186fd}"
region="${AWS_REGION:-ap-southeast-7}"
compose=compose.staging.yaml
dir="" tag="" healthy=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) dir="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --compose) compose="$2"; shift 2 ;;
    --healthy) healthy="$2"; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ -n "$dir" && -n "$tag" && -f "$compose" ]] || { echo "need --dir, --tag and an existing $compose" >&2; exit 2; }

state="$(aws ec2 describe-instances --instance-ids "$instance" --query 'Reservations[0].Instances[0].State.Name' --output text)"
if [[ "$state" != running ]]; then
  echo "cdp-stg is $state (office hours Mon-Fri 08:00-20:00 ICT). Start it with:" >&2
  echo "  aws lambda invoke --function-name cdp-stg-power --cli-binary-format raw-in-base64-out --payload '{\"action\":\"start\"}' /dev/stdout" >&2
  exit 1
fi

account="$(aws sts get-caller-identity --query Account --output text)"
registry="${account}.dkr.ecr.${region}.amazonaws.com"
compose_b64="$(base64 < "$compose" | tr -d '\n')"
script="$(cat <<EOS
set -eu
cd '$dir'
echo '$compose_b64' | base64 -d > compose.staging.yaml
printf 'REGISTRY=%s\nIMAGE_TAG=%s\n' '$registry' '$tag' > .env
aws ecr get-login-password --region '$region' | docker login --username AWS --password-stdin '$registry' >/dev/null
docker compose -f compose.staging.yaml pull --quiet
docker compose -f compose.staging.yaml up -d --remove-orphans
for svc in $healthy; do
  id=\$(docker compose -f compose.staging.yaml ps -q "\$svc")
  for i in \$(seq 1 60); do
    st=\$(docker inspect -f '{{.State.Health.Status}}' "\$id")
    [ "\$st" = healthy ] && break
    [ "\$st" = unhealthy ] && { echo "\$svc unhealthy"; docker compose -f compose.staging.yaml logs --tail 40 "\$svc"; exit 1; }
    sleep 5
  done
  [ "\$st" = healthy ] || { echo "\$svc not healthy after 300s (\$st)"; exit 1; }
done
docker compose -f compose.staging.yaml ps --format '{{.Service}} {{.Image}} {{.Status}}'
docker image prune -f >/dev/null
EOS
)"
params="$(jq -nc --arg s "$script" '{commands: [$s]}')"
command_id="$(aws ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript \
  --comment "deploy $(basename "$dir") $tag" --timeout-seconds 900 \
  --parameters "$params" --query Command.CommandId --output text)"
echo "ssm command $command_id"

for _ in $(seq 1 120); do
  status="$(aws ssm get-command-invocation --command-id "$command_id" --instance-id "$instance" \
    --query Status --output text 2>/dev/null || echo Pending)"
  case "$status" in Pending|InProgress|Delayed) sleep 10 ;; *) break ;; esac
done
aws ssm get-command-invocation --command-id "$command_id" --instance-id "$instance" \
  --query '[StandardOutputContent,StandardErrorContent]' --output text
echo "deploy status: $status"
[[ "$status" == Success ]]
