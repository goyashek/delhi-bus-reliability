#!/bin/zsh
set -eu

backup_home=${DELHI_BUS_BACKUP_HOME:-${0:A:h:h}}
source "$backup_home/.env"

: "${R2_ACCESS_KEY_ID:?Add R2_ACCESS_KEY_ID to .env}"
: "${R2_SECRET_ACCESS_KEY:?Add R2_SECRET_ACCESS_KEY to .env}"

export RCLONE_CONFIG_DELBUS_TYPE=s3
export RCLONE_CONFIG_DELBUS_PROVIDER=Cloudflare
export RCLONE_CONFIG_DELBUS_REGION=auto
export RCLONE_CONFIG_DELBUS_ENDPOINT=https://2b7fb6699a092f387a427b1b476c6f92.r2.cloudflarestorage.com
export RCLONE_CONFIG_DELBUS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID
export RCLONE_CONFIG_DELBUS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY

destination="$backup_home/data/raw/vehicle_positions_pb"
mkdir -p "$destination"

# ponytail: append-only object keys, skip existing files; run a full check if corruption is found.
/opt/homebrew/bin/rclone copy \
  delbus:delhi-bus-vehicle-positions/vehicle_positions \
  "$destination" \
  --fast-list \
  --ignore-existing \
  --transfers 2 \
  --checkers 4 \
  --retries 5 \
  --low-level-retries 10

date -u "+%Y-%m-%dT%H:%M:%SZ" > "$destination/.last_success"
