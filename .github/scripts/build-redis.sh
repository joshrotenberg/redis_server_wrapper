#!/usr/bin/env bash
set -euo pipefail

version="$1"
prefix="$2"

if [[ -x "${prefix}/bin/redis-server" && -x "${prefix}/bin/redis-cli" ]]; then
  exit 0
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

curl --fail --location --silent --show-error \
  "https://download.redis.io/releases/redis-${version}.tar.gz" \
  --output "${work_dir}/redis.tar.gz"

tar -xzf "${work_dir}/redis.tar.gz" --directory "${work_dir}"
make -C "${work_dir}/redis-${version}" -j"$(nproc)"

mkdir -p "${prefix}/bin" "${prefix}/include"
cp "${work_dir}/redis-${version}/src/redis-server" "${prefix}/bin/"
cp "${work_dir}/redis-${version}/src/redis-cli" "${prefix}/bin/"
cp "${work_dir}/redis-${version}/src/redismodule.h" "${prefix}/include/"
