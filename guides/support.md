# Support policy

This document defines the compatibility contract for RedisServerWrapper. A
version may work outside these ranges, but it is not considered supported until
it is represented in CI or explicitly listed here.

## Runtime matrix

| Component | Supported | Verification |
| --- | --- | --- |
| Redis | 7.2, 7.4, 8.2, and the latest Redis 8.x line | The full standalone, Cluster, Sentinel, TLS, and custom-module suite runs on Redis 7.2.14, 7.4.9, 8.2.6, and 8.8.0. Patch releases within a listed line are supported. |
| Elixir / OTP | Elixir 1.19+ | CI verifies Elixir 1.19 / OTP 27 and Elixir 1.20 / OTP 29. |
| Linux | Supported | Every CI job runs on GitHub-hosted Ubuntu. |
| macOS | Supported for local development and testing | The suite is maintainer-tested on macOS. Port 7000 is avoided because AirPlay Receiver commonly owns it. |
| Windows | Not supported natively | Process lifecycle and Sentinel use POSIX process behavior and `/bin/sh`. WSL may work as Linux, but is not in CI. |

`redis-server` and `redis-cli` must be mutually compatible and available on
`PATH`, unless explicit binary paths are provided. `kill` is required by
`RedisServerWrapper.Chaos`. `lsof` and `kill` improve ownership checks and
forced teardown; ordinary managed lifecycle paths degrade safely if they are
missing.

The optional `managed: :forcola` backend is POSIX-only. It provides stronger
child-process cleanup than the default Port backend.

## Redis distributions

Redis 8's Full distribution uses the same `redis-server` executable as Redis
Open Source. RedisServerWrapper therefore treats `:core` and `:full` the same
at process launch: both resolve `redis-server` from `PATH`. Modules bundled
with a Full installation remain Redis's responsibility.

Legacy Redis Stack is different. Select it explicitly:

```elixir
RedisServerWrapper.start_server(
  distribution: :legacy_stack,
  redis_server_bin: "/opt/redis-stack/bin/redis-server"
)
```

When `distribution: :legacy_stack` is selected, the wrapper may discover
legacy sibling modules next to the Stack server binary. It never performs that
discovery for `:core` or `:full`. If `:redis_server_bin` is omitted, legacy
selection uses `REDIS_LEGACY_STACK_SERVER_BIN` and then falls back to
`redis-stack-server`.

An explicit `:redis_server_bin` always wins. Explicit `:loadmodule` entries are
honored for every distribution.

## Topology and transport support

| Surface | TCP | TLS | Unix socket | ACL username + password |
| --- | --- | --- | --- | --- |
| Server | Yes | Yes, including TLS-only and mTLS | Yes, including Unix-only | Yes |
| Cluster | Yes | Yes, TLS-only nodes | No | Yes |
| Sentinel | Yes | Yes, TLS-only data and control nodes | No | Yes |
| Manager basic | Yes | Yes | Yes | Yes |
| Manager Cluster / Sentinel | Yes | Yes | No | Yes |

Cluster and Sentinel reject Unix sockets because their members must advertise
and reach network endpoints. `:control_host` is used for readiness checks,
replication, Cluster announcements, and Sentinel monitoring; set it explicitly
when the first bind address is a wildcard.

See the [configuration reference](configuration.md) for option-level details
and the [testing guide](testing.md) for required data, Cluster bus, replica, and
Sentinel ports.

## External Redis modules

Redis modules are native shared libraries. Compatibility depends on all of:

- the operating system and CPU architecture;
- the Redis Module API used to build the module;
- the Redis major/minor line;
- the module's own load arguments and dependencies.

RedisServerWrapper safely writes module paths and arguments, but cannot make an
incompatible binary loadable. Build or obtain a module for the same platform
and a Redis version supported by that module, then test `MODULE LIST` and the
module's commands on every Redis line you plan to deploy.

The repository compiles and runs the shipped
`test/support/version_matrix_module.c` example across every Redis version in
the CI matrix. The `examples/event_stream.exs` demo uses the external
`redis-event-stream-module`; that module must be built separately for the
target Redis and platform.
