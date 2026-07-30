# Configuration and option reference

RedisServerWrapper has three configuration layers:

1. `RedisServerWrapper.Config` is a typed Redis configuration builder.
2. Server, Cluster, and Sentinel accept curated operational options and own
   lifecycle/topology directives.
3. `:extra` is a token-safe escape hatch for Redis directives that do not have
   typed fields.

The wrapper does not claim that every Redis directive is a typed option. Use
the tables below to determine what is emitted, what controls `redis-cli`, and
what a topology owns.

## Typed Config options

`RedisServerWrapper.Config.new/1` validates these keys and
`RedisServerWrapper.Config.to_config_string/1` renders the Redis directives.
Unless noted, the same key can be passed to
`RedisServerWrapper.Server.start/1` or `RedisServerWrapper.Server.start_link/1`.

### Endpoint, auth, and logging

| Option | Default | Meaning |
| --- | --- | --- |
| `port` | `6379` | Plain TCP listener, `0..65_535`; `0` disables plaintext TCP. |
| `bind` | `"127.0.0.1"` | One address, a whitespace-separated string, or a list of addresses. |
| `control_host` | `nil` | Concrete host used by `redis-cli` and lifecycle probes. Derived from the first bind address; required when that address is a wildcard. Not emitted to Redis. |
| `username` | `nil` | Named ACL user. Requires a string `password`; disables the default user unless the name is `"default"`. |
| `password` | `nil` | `requirepass` when no username is set, otherwise the named ACL user's password. |
| `loglevel` | `:notice` | One of `:debug`, `:verbose`, `:notice`, or `:warning`. |
| `logfile` | `nil` | Redis log path. Server supplies a generated private node-directory path when omitted. |
| `daemonize` | `false` | Direct Config value. Server overrides it from `managed`: foreground for `true`/`:forcola`, daemonized for `false`. |
| `pidfile` | `nil` | Direct Config value. Server always supplies a generated node-directory path. |
| `dir` | `nil` | Redis working directory. Server always supplies its generated node directory. |

At least one of `port > 0`, `tls_port`, or `unixsocket` must be enabled.

### Persistence, memory, and TCP

| Option | Default | Meaning |
| --- | --- | --- |
| `save` | `:default` | `:default` leaves Redis defaults untouched; `:disabled` emits an empty `save`; a list of `{positive_seconds, positive_changes}` emits explicit policies. |
| `appendonly` | `false` | Enables AOF persistence. |
| `appendfsync` | `:everysec` | One of `:always`, `:everysec`, or `:no`. |
| `maxmemory` | `nil` | Redis memory limit string, for example `"256mb"`. |
| `maxmemory_policy` | `nil` | Redis eviction policy string, for example `"allkeys-lru"`. |
| `tcp_backlog` | `nil` | Non-negative Redis TCP listen backlog. |
| `timeout` | `nil` | Redis idle-client timeout when using `Config` directly. On `Server`, the `timeout` keyword is instead the wrapper startup timeout; use of the Redis directive through Server is intentionally unavailable because `:extra` cannot override wrapper-owned `timeout`. |
| `tcp_keepalive` | `nil` | Non-negative Redis TCP keepalive interval. |
| `unixsocket` | `nil` | Standalone Unix socket path, at most 103 bytes for portable macOS/Linux support. |
| `unixsocketperm` | `nil` | Redis socket mode string, for example `"700"`. |

### TLS

| Option | Default | Meaning |
| --- | --- | --- |
| `tls_port` | `nil` | TLS listener, `1..65_535`. Enables TLS configuration and makes TLS the lifecycle connection. |
| `tls_cert_file` | `nil` | Required TLS server certificate when `tls_port` is set. |
| `tls_key_file` | `nil` | Required TLS server private key when `tls_port` is set. |
| `tls_ca_cert_file` | `nil` | Trusted CA file used by Redis and `redis-cli`; mutually exclusive with `tls_ca_cert_dir`. |
| `tls_ca_cert_dir` | `nil` | Trusted CA directory; mutually exclusive with `tls_ca_cert_file`. Exactly one CA source is required. |
| `tls_auth_clients` | `nil` | `nil`, `"yes"`, `"no"`, or `"optional"`. Redis's default is client-certificate authentication. |
| `tls_client_cert_file` | `nil` | `redis-cli` client certificate. Required with its key when client auth is `nil`/`"yes"`. Not emitted to Redis. |
| `tls_client_key_file` | `nil` | `redis-cli` client private key. Not emitted to Redis. |
| `tls_server_name` | `nil` | `redis-cli` SNI/hostname-verification name. Not emitted to Redis. |
| `tls_insecure` | `false` | Disables `redis-cli` certificate verification. Not emitted to Redis. |
| `tls_replication` | `false` | Emits `tls-replication yes`; Sentinel sets this for TLS data nodes. |
| `tls_cluster` | `false` | Emits `tls-cluster yes`; Cluster sets this for TLS nodes. |

To run TLS-only, set `port: 0`. To use both plaintext and TLS listeners, leave a
positive `port`; lifecycle commands still use TLS whenever `tls_port` is set.

### Replication and Cluster directives

| Option | Default | Meaning |
| --- | --- | --- |
| `replicaof` | `nil` | `{host, port}` for a standalone replica. Sentinel owns this for its replicas. |
| `masteruser` | `nil` | ACL username used for replication; requires `replicaof` and `masterauth`. |
| `masterauth` | `nil` | Replication password; requires `replicaof`. |
| `cluster_enabled` | `false` | Emits `cluster-enabled yes`. Cluster owns this for its nodes. |
| `cluster_config_file` | `nil` | Redis Cluster state filename. Cluster generates a per-node filename. |
| `cluster_node_timeout` | `nil` | Non-negative Cluster node timeout in milliseconds. |
| `cluster_announce_hostname` | `nil` | Explicit Cluster announcement hostname. |
| `cluster_announce_port` | `nil` | Explicit Cluster data port, `1..65_535`. |
| `cluster_announce_bus_port` | `nil` | Explicit Cluster bus port, `1..65_535`. |

### Modules and raw directives

| Option | Default | Meaning |
| --- | --- | --- |
| `loadmodule` | `[]` | Module paths or `{path, [string_args]}` tuples. Every token is quoted/escaped when needed. |
| `extra` | `[]` | `{directive, string_or_nonempty_string_vector}` tuples for untyped Redis directives. |

Use an argument vector for multi-token directives:

```elixir
extra: [
  {"notify-keyspace-events", "KEA"},
  {"client-output-buffer-limit", ["pubsub", "32mb", "8mb", "60"]}
]
```

Directive names are case-insensitive and must contain letters, numbers, and
hyphens. `:extra` cannot replace wrapper-owned fields, including endpoint,
auth, TLS, persistence, replication, Cluster, module, PID, directory, and
process-lifecycle directives. Unknown Redis directives are accepted by the
builder and may still be rejected by the selected Redis version.

## Server options

`Server.start/1` and `Server.start_link/1` accept the Config keys above plus:

| Option | Default | Meaning |
| --- | --- | --- |
| `name` | `nil` | GenServer registration name. Removed before Config construction. |
| `redis_server_bin` | distribution default | Executable name or path. |
| `redis_cli_bin` | `"redis-cli"` | Executable name or path used for readiness, commands, and shutdown. |
| `distribution` | `:core` | `:core`, `:full`, or `:legacy_stack`; see the [support policy](support.md). |
| `timeout` | `10_000` | Wrapper startup/readiness timeout in milliseconds. This shadows the Config field with the same name. |
| `managed` | `true` | `true` for a foreground Port, `:forcola` for guaranteed owner-death cleanup, or `false` for a detached Redis daemon. |

Server owns `daemonize`, `pidfile`, and `dir` even if values are supplied. It
honors an explicit `logfile`, otherwise it generates one beside `redis.conf`.
Only `managed: false` can be detached with `Server.detach/1`.

## Cluster options

Cluster accepts exactly this topology surface; it does not forward every Config
field.

| Option | Default | Meaning |
| --- | --- | --- |
| `name` | `nil` | Cluster GenServer registration name. |
| `masters` | `3` | Positive master count. |
| `replicas_per_master` | `0` | Non-negative replica count for each master. |
| `base_port` | `7100` | First data/TLS port. Nodes use consecutive ports. |
| `bind` | `"127.0.0.1"` | Node listen address(es). |
| `control_host` | first bind | Concrete host for commands and Cluster announcements. |
| `username` / `password` | `nil` | Password-only or named ACL authentication on every node. |
| `tls` | `false` | Creates TLS-only Cluster nodes when true. |
| TLS certificate/client options | `nil` / `false` | `tls_cert_file`, `tls_key_file`, one CA source, `tls_auth_clients`, client certificate/key, `tls_server_name`, and `tls_insecure`. |
| `redis_server_bin` | distribution default | Redis executable name/path. |
| `redis_cli_bin` | `"redis-cli"` | CLI executable name/path. |
| `distribution` | `:core` | `:core`, `:full`, or `:legacy_stack`. |
| `timeout` | `10_000` | Per-node process-readiness timeout. |
| `convergence_timeout` | value of `timeout` | Bounded wait for every node to report a healthy agreed topology. |
| `cluster_node_timeout` | `5_000` | Positive Cluster node timeout in milliseconds. |
| `loadmodule` | `[]` | Modules loaded into every node. |
| `extra` | `[]` | Raw non-reserved Redis directives loaded into every node. |
| `managed` | `true` | Lifecycle backend forwarded to every node. Only `false` supports `Cluster.detach/1`. |

For `N = masters * (1 + replicas_per_master)`, Cluster uses data ports
`base_port..base_port+N-1` and bus ports exactly 10,000 higher. Both ranges are
validated for overlap and overflow. Unix sockets are rejected.

## Sentinel options

Sentinel accepts exactly this topology surface:

| Option | Default | Meaning |
| --- | --- | --- |
| `name` | `nil` | Sentinel topology GenServer registration name. |
| `master_name` | `"mymaster"` | Name monitored by Sentinel. |
| `master_port` | `6390` | Master data/TLS port. |
| `replicas` | `2` | Non-negative replica count. |
| `replica_base_port` | `master_port + 1` | First consecutive replica port. |
| `sentinels` | `3` | Positive Sentinel control-process count. |
| `sentinel_base_port` | `26_389` | First consecutive Sentinel control port. |
| `quorum` | `2` | Positive quorum, no greater than `sentinels`. |
| `down_after_ms` | `5_000` | Sentinel down-after-milliseconds value. |
| `failover_timeout_ms` | `10_000` | Sentinel failover-timeout value. |
| `bind` | `"127.0.0.1"` | Listen address(es) for data and control processes. |
| `control_host` | first bind | Concrete host used for replication, monitoring, and commands. |
| `username` / `password` | `nil` | Password-only or named ACL auth for data/control nodes and monitoring. |
| `tls` | `false` | Creates TLS-only Redis and Sentinel listeners when true. |
| TLS certificate/client options | `nil` / `false` | Same TLS fields accepted by Cluster. |
| `redis_server_bin` | distribution default | Used to start both Redis and `--sentinel` processes. |
| `redis_cli_bin` | `"redis-cli"` | CLI executable name/path. |
| `distribution` | `:core` | `:core`, `:full`, or `:legacy_stack`. |
| `timeout` | `10_000` | Per-process readiness timeout. |
| `convergence_timeout` | value of `timeout` | Bounded replication and Sentinel-discovery wait. |
| `loadmodule` | `[]` | Modules loaded into master and replicas, never Sentinel control processes. |
| `managed` | `true` | Lifecycle backend used by data and control processes. Only `false` supports `Sentinel.detach/1`. |

All master, replica, and Sentinel port ranges must be valid, free, and
non-overlapping. Unix sockets and arbitrary `:extra` data-node directives are
not part of the Sentinel surface.

## Manager options

Manager always launches detached (`managed: false`) processes and records their
credentials/endpoints in its state file. It intentionally exposes a narrower
surface than Server, Cluster, and Sentinel.

### `Manager.start_basic/1`

| Option | Default | Meaning |
| --- | --- | --- |
| `name` | generated | Persistent instance name. |
| `port` | `6379` | Plain listener; use `0` with TLS or a Unix socket. |
| `password` | generated | Omit to generate; pass `nil` explicitly for no auth. |
| `username` | `nil` | Named ACL user paired with password. |
| `bind` / `control_host` | loopback / derived | Server listen and control addresses. |
| `persist` | `false` | Enables default RDB policy and AOF. |
| `maxmemory` | `nil` | Redis memory limit. |
| `loadmodule` | `[]` | Modules loaded into the server. |
| `distribution` | `:core` | Redis distribution selection. |
| `extra` | `[]` | Raw non-reserved Redis directives. |
| Unix/TLS options | unset | `unixsocket`, `unixsocketperm`, `tls_port`, certificate/CA/client fields, `tls_server_name`, and `tls_insecure`. |

The `tls` boolean is ignored for basic instances; setting `tls_port` selects
TLS. Manager does not forward custom binary paths, startup timeouts, or other
typed Config fields.

### `Manager.start_cluster/1`

Accepted keys are `name`, `masters`, `replicas_per_master`, `base_port`,
`password`, `username`, `bind`, `control_host`, `loadmodule`, `distribution`,
`tls`, and the TLS certificate/client fields. Defaults match Cluster. Manager
does not forward Cluster `extra`, timeout, node-timeout, binary, or managed
options.

### `Manager.start_sentinel/1`

Accepted keys are `name`, `master_port`, `replicas`, `sentinels`, `quorum`,
`sentinel_base_port`, `password`, `username`, `bind`, `control_host`,
`loadmodule`, `distribution`, `tls`, and the TLS certificate/client fields.
Defaults match Sentinel except Manager derives quorum as `min(2, sentinels)`.
Manager does not forward `replica_base_port`, timing, binary, raw Config, or
managed options.

### Manager state and operations

The application environment key
`:redis_server_wrapper, :manager_state_file` changes the default
`~/.config/redis-server-wrapper/instances.json` path.

The public operations are:

- `list/1` (optionally `:basic`, `:cluster`, or `:sentinel`);
- `info/1` for stored data plus live status;
- `credentials/1` for intentionally retrieving unredacted auth/URL data;
- `stop/1` and `stop_all/0` for ownership-checked destructive shutdown;
- `cleanup/0` for removing stopped entries without signaling live processes.

See [security and data safety](security.md) before using persistent Manager
instances outside an isolated developer machine.
