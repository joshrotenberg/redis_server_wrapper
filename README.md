# RedisServerWrapper

[![Hex.pm](https://img.shields.io/hexpm/v/redis_server_wrapper.svg)](https://hex.pm/packages/redis_server_wrapper)
[![Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/redis_server_wrapper)

Manage `redis-server` processes from Elixir -- single instances, clusters, and
sentinel topologies with GenServer lifecycle management.

No Docker required. Just `redis-server` and `redis-cli` on your PATH.

## Installation

Add `redis_server_wrapper` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:redis_server_wrapper, "~> 0.7"}
  ]
end
```

## Prerequisites

You need `redis-server` and `redis-cli` installed and available on your PATH.
The wrapper uses `kill` and `lsof` for process inspection and management
when they are present, but normal server, cluster, and sentinel lifecycle
operations degrade safely when minimal runtime images omit them. Occupied ports
fail startup; the wrapper does not shut down or signal an existing listener to
claim a port. Signal-based functions in `RedisServerWrapper.Chaos` still require
`kill`.

```bash
# macOS
brew install redis

# Ubuntu/Debian
sudo apt-get install redis-server

# Verify
redis-server --version
```

### Supported Redis versions and distributions

The supported Redis lines are 7.2, 7.4, 8.2, and the latest Redis 8.x release.
CI runs the full standalone, Cluster, and Sentinel suite against a current patch
release from each line, including a custom-module startup test. New patch
releases within those lines are supported; older releases may work but are not
part of the compatibility contract.

Startup uses the core `redis-server` on PATH by default:

```elixir
# Redis Open Source / core (default)
RedisServerWrapper.start_server(distribution: :core)

# Redis 8 full distribution; modules are managed by Redis itself
RedisServerWrapper.start_server(distribution: :full)

# Legacy Redis Stack only; enables sibling-module discovery
RedisServerWrapper.start_server(
  distribution: :legacy_stack,
  redis_server_bin: "/path/to/redis-stack/bin/redis-server"
)
```

An explicit `:redis_server_bin` always wins. Legacy Stack selection can also use
the `REDIS_LEGACY_STACK_SERVER_BIN` environment variable. The wrapper never
silently switches distributions, and it only discovers sibling Stack modules
when `distribution: :legacy_stack` is explicit. Caller-provided `:loadmodule`
entries work with every distribution.

## Quick Start

### Single Server

```elixir
{:ok, server} = RedisServerWrapper.start_server(port: 6400, password: "secret")

RedisServerWrapper.Server.ping(server)
#=> true

RedisServerWrapper.Server.run(server, ["SET", "key", "value"])
#=> {:ok, "OK"}

RedisServerWrapper.Server.run(server, ["GET", "key"])
#=> {:ok, "value"}

RedisServerWrapper.Server.stop(server)
```

### Cluster

```elixir
{:ok, cluster} = RedisServerWrapper.start_cluster(masters: 3, base_port: 7100)

RedisServerWrapper.Cluster.healthy?(cluster)
#=> true

RedisServerWrapper.Cluster.node_addrs(cluster)
#=> ["127.0.0.1:7100", "127.0.0.1:7101", "127.0.0.1:7102"]

RedisServerWrapper.Cluster.stop(cluster)
```

### Sentinel

```elixir
{:ok, sentinel} = RedisServerWrapper.start_sentinel(
  master_port: 6390,
  replicas: 2,
  sentinels: 3
)

RedisServerWrapper.Sentinel.healthy?(sentinel)
#=> true

RedisServerWrapper.Sentinel.master_addr(sentinel)
#=> "127.0.0.1:6390"

RedisServerWrapper.Sentinel.stop(sentinel)
```

### Persistent Instances (Manager)

The `Manager` tracks instances across IEx sessions using a JSON state file:

```elixir
RedisServerWrapper.Manager.start_basic(name: "dev-redis", port: 6400)
#=> {:ok, %{name: "dev-redis", url: "redis://:password@127.0.0.1:6400", ...}}

RedisServerWrapper.Manager.list()
#=> [%{name: "dev-redis", type: :basic, ...}]

RedisServerWrapper.Manager.credentials("dev-redis")
#=> {:ok, %{password: "password", url: "redis://:password@127.0.0.1:6400"}}

RedisServerWrapper.Manager.stop("dev-redis")
```

Manager-generated passwords come from the operating system's cryptographic
random source. Console output redacts credentials; use `Manager.credentials/1`
when plaintext access is intentional. The Manager state directory and
credential-bearing Redis configuration directories are created with mode
`0700`, while state and configuration files use `0600`. State updates are
atomic and serialized across connected BEAM nodes. Independent, unconnected
BEAM instances must not write to the same Manager state file concurrently.
Invalid state is moved aside as an `instances.json.corrupt-*` recovery copy
instead of being overwritten.

### Managed Process Lifecycle

`Server.start_link/1` accepts a `:managed` option controlling how the
`redis-server` OS process is tied to the BEAM:

- `managed: true` (default) - `redis-server` runs as a foreground Port. Teardown
  runs in `terminate/2`, which OTP skips on a `:brutal_kill` supervisor shutdown
  or a hard BEAM death (SIGKILL, OOM), so the OS process can be stranded on those
  paths, keeping its port bound.
- `managed: :forcola` - `redis-server` runs in the foreground under a
  [`Forcola.Daemon`](https://github.com/joshrotenberg/forcola), whose Rust shim
  guarantees the OS process group is killed and confirmed dead on owner death or
  supervisor shutdown, including the paths where `terminate/2` never runs. This
  requires the optional dependency and is opt-in:

  ```elixir
  # mix.exs
  {:forcola, "~> 0.3"}
  ```

  ```elixir
  {:ok, server} = RedisServerWrapper.Server.start_link(port: 6400, managed: :forcola)
  ```

  Without `:forcola` on the path, `start_link` returns
  `{:error, :forcola_not_available}`. Forcola is POSIX-only and ships precompiled,
  SHA256-verified shim binaries, so it needs no Rust toolchain at build time.
- `managed: false` - `redis-server` daemonizes independently; the caller owns the
  OS process lifecycle.

If a managed Redis process exits unexpectedly, its `Server` GenServer now exits
with an abnormal `{:redis_server_exit, backend, reason}` reason. A normal OTP
supervisor therefore restarts the complete Server child instead of retaining a
live GenServer with a missing OS process. Port and Forcola backends follow the
same contract.

Cluster and Sentinel make topology loss explicit:

- Cluster removes a dead node from `Cluster.nodes/1`, stays available for chaos
  and failover work, and reports both `all_alive?/1` and `healthy?/1` as false.
  `healthy?/1` validates every remaining node's state, slot coverage, failure
  counts, known-node count, and master count.
- Sentinel uses the selected managed backend for its control processes as well
  as its data nodes. Loss of a master, replica, or Sentinel control process
  stops the topology abnormally after cleaning up the remaining children, so a
  supervisor can restart the complete topology from a coherent state.

Cluster formation, replica linkage, and Sentinel discovery use bounded polling
instead of fixed startup sleeps. Set `convergence_timeout: milliseconds` to
override the default (the value of `:timeout`). Timeout errors include the last
observed health state to make transitional or malformed Redis output diagnosable.

### Custom Modules

Use `:loadmodule` to load one or more Redis modules. A plain path is enough for
a module with defaults; use `{path, [args]}` when it takes load-time arguments.
The structured form quotes every path and argument safely in the generated
`redis.conf`.

```elixir
module_path = System.fetch_env!("EVENT_STREAM_MODULE")

{:ok, server} =
  RedisServerWrapper.start_server(
    port: 6460,
    loadmodule: [
      {module_path, ["events", "expired,set", "maxlen", "1000"]}
    ]
  )

RedisServerWrapper.Server.run(server, ["MODULE", "LIST"])
```

For a cluster, the same modules are loaded into every node. For a Sentinel
topology, they are loaded into the master and every replica (not the Sentinel
control processes):

```elixir
RedisServerWrapper.start_cluster(
  base_port: 7100,
  loadmodule: [
    {module_path, ["cluster-streams", "per-node"]}
  ]
)
```

The original raw directive form remains supported for compatibility, such as
`loadmodule: ["/path/module.so events expired"]`, but the structured form is
recommended when arguments are present.

For a complete event-stream demo using
[`redis-event-stream-module`](https://github.com/joshrotenberg/redis-event-stream-module):

```bash
EVENT_STREAM_MODULE=/absolute/path/to/libredis_event_stream_module.so \
  mix run examples/event_stream.exs
```

## Configuration

All Redis configuration values are encoded as individual Redis tokens, including
paths, passwords, module arguments, and `:extra` directives. Use a string for a
single extra argument or a list for a multi-argument directive. Extras cannot
override wrapper-owned lifecycle or transport settings such as `port`, `bind`,
`daemonize`, `pidfile`, or `dir`.

```elixir
RedisServerWrapper.start_server(
  port: 6400,
  password: "secret",
  maxmemory: "256mb",
  maxmemory_policy: "allkeys-lru",
  extra: [
    {"notify-keyspace-events", "KEA"},
    {"client-output-buffer-limit", ["pubsub", "32mb", "8mb", "60"]},
    {"hz", "100"}
  ]
)
```

`bind` may be one address, a whitespace-separated string, or a list of listen
addresses. `control_host` independently selects the address used by
`redis-cli`, readiness probes, replication, Cluster, and Sentinel:

```elixir
RedisServerWrapper.start_server(
  port: 6400,
  bind: ["127.0.0.1", "::1"],
  control_host: "127.0.0.1"
)
```

### TCP, TLS, Unix sockets, and ACL authentication

Lifecycle operations use a shared `RedisServerWrapper.Connection`, so startup
readiness, commands, health checks, shutdown, Cluster, Sentinel, and Manager
all retain the same endpoint and credentials.

Set `username` with `password` to configure and use a named ACL user. Without a
username, `password` retains its password-only `requirepass` behavior:

```elixir
RedisServerWrapper.start_server(
  port: 6400,
  username: "demo-app",
  password: "secret"
)
```

Disable TCP with `port: 0` for a Unix-only server:

```elixir
RedisServerWrapper.start_server(
  port: 0,
  unixsocket: "/tmp/demo-redis.sock",
  unixsocketperm: "700"
)
```

For a TLS-only server, set `port: 0`, a `tls_port`, and the server identity.
The CA file or directory is also used by `redis-cli` for verification:

```elixir
RedisServerWrapper.start_server(
  port: 0,
  tls_port: 6400,
  tls_cert_file: "/certs/server.crt",
  tls_key_file: "/certs/server.key",
  tls_ca_cert_file: "/certs/ca.crt",
  tls_auth_clients: "no",
  tls_server_name: "redis.demo.test"
)
```

Use `tls_ca_cert_dir` instead of `tls_ca_cert_file` for a CA directory.
Mutual-TLS clients can set `tls_client_cert_file` and
`tls_client_key_file`. `tls_insecure: true` explicitly disables redis-cli
certificate verification and is intended only for controlled tests.

Cluster and Sentinel use TLS-only nodes when `tls: true`; pass the same
certificate options:

```elixir
RedisServerWrapper.start_cluster(
  masters: 3,
  base_port: 7100,
  tls: true,
  tls_cert_file: "/certs/server.crt",
  tls_key_file: "/certs/server.key",
  tls_ca_cert_file: "/certs/ca.crt",
  tls_auth_clients: "no"
)
```

The wrapper enables `tls-cluster` or `tls-replication` as required. Sentinel
also configures `sentinel auth-user`/`auth-pass` for named ACL credentials.
Unix sockets are a standalone-server transport; Cluster and Sentinel reject
them immediately because their nodes must communicate over network ports.

### Chaos / Fault Injection

The `Chaos` module provides fault injection primitives for testing resilience:

```elixir
alias RedisServerWrapper.{Chaos, Cluster, Server}

{:ok, cluster} = RedisServerWrapper.start_cluster(
  masters: 3,
  replicas_per_master: 1,
  base_port: 7100
)

# Kill the master owning a key's hash slot
{:ok, killed} = Chaos.kill_master(cluster, "user:123")

# Kill by slot number
{:ok, killed} = Chaos.kill_master(cluster, 5000)

# Freeze a node (SIGSTOP) and resume later
{:ok, os_pid} = Chaos.freeze_node(server)
# ... test timeout/retry behavior ...
Chaos.resume_node(os_pid)

# Freeze with automatic resume after a duration
Chaos.pause_node(server, 5_000)

# Simulate a network partition
nodes = Cluster.nodes(cluster)
{active, isolated} = Enum.split(nodes, 2)
{:ok, frozen_pids} = Chaos.partition(cluster, [active, isolated])

# Recover all frozen nodes
Chaos.recover(frozen_pids)

# Other tools
Chaos.slow_down(server, 2_000)        # CLIENT PAUSE
Chaos.flushall(server)                 # wipe all data
Chaos.fill_memory(server, 10_000)      # fill with 1KB dummy keys
Chaos.trigger_save(server)             # force BGSAVE
Chaos.random_kill(cluster)             # kill a random node
Chaos.trigger_failover(replica)        # CLUSTER FAILOVER on a replica
```

## Use Cases

- **Testing** -- spin up real Redis instances in ExUnit setup/teardown
- **Development** -- run Redis alongside your app without system services
- **CI** -- no Docker layer needed, just install redis-server
- **Cluster/Sentinel testing** -- create full topologies in a single function call
- **Resilience testing** -- fault injection with the Chaos module (kill nodes, simulate partitions, inject latency)

## License

Licensed under either of

- MIT License
- Apache License, Version 2.0

at your option. See [LICENSE](LICENSE) for details.
