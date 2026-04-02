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
    {:redis_server_wrapper, "~> 0.1.0"}
  ]
end
```

## Prerequisites

You need `redis-server` and `redis-cli` installed and available on your PATH.

```bash
# macOS
brew install redis

# Ubuntu/Debian
sudo apt-get install redis-server

# Verify
redis-server --version
```

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
{:ok, cluster} = RedisServerWrapper.start_cluster(masters: 3, base_port: 7000)

RedisServerWrapper.Cluster.healthy?(cluster)
#=> true

RedisServerWrapper.Cluster.node_addrs(cluster)
#=> ["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"]

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

RedisServerWrapper.Manager.stop("dev-redis")
```

## Configuration

All Redis configuration directives can be set via the `Config` struct options.
Use the `:extra` option as an escape hatch for any directive not covered by
the typed fields:

```elixir
RedisServerWrapper.start_server(
  port: 6400,
  password: "secret",
  maxmemory: "256mb",
  maxmemory_policy: "allkeys-lru",
  extra: [
    {"notify-keyspace-events", "KEA"},
    {"hz", "100"}
  ]
)
```

### Chaos / Fault Injection

The `Chaos` module provides fault injection primitives for testing resilience:

```elixir
alias RedisServerWrapper.{Chaos, Cluster, Server}

{:ok, cluster} = RedisServerWrapper.start_cluster(
  masters: 3,
  replicas_per_master: 1,
  base_port: 7000
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
