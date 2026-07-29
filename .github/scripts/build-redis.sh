#!/usr/bin/env bash
set -euo pipefail

version="$1"
prefix="$2"
build_flavor="tls-v1"
build_flavor_file="${prefix}/.build-flavor"

if [[ -x "${prefix}/bin/redis-server" &&
      -x "${prefix}/bin/redis-cli" &&
      -f "${build_flavor_file}" &&
      "$(cat "${build_flavor_file}")" == "${build_flavor}" ]]; then
  exit 0
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

curl --fail --location --silent --show-error \
  "https://download.redis.io/releases/redis-${version}.tar.gz" \
  --output "${work_dir}/redis.tar.gz"

tar -xzf "${work_dir}/redis.tar.gz" --directory "${work_dir}"
make -C "${work_dir}/redis-${version}" BUILD_TLS=yes -j"$(nproc)"

mkdir -p "${prefix}/bin" "${prefix}/include"
cp "${work_dir}/redis-${version}/src/redis-server" "${prefix}/bin/"
cp "${work_dir}/redis-${version}/src/redis-cli" "${prefix}/bin/"
cp "${work_dir}/redis-${version}/src/redismodule.h" "${prefix}/include/"
printf '%s\n' "${build_flavor}" > "${build_flavor_file}"
