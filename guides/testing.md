# Testing and troubleshooting

RedisServerWrapper is designed for integration tests that need a real Redis
process without adding a Docker dependency. The examples below keep process
ownership and port allocation explicit.

## ExUnit fixture

For the simplest and most predictable suite, keep Redis-owning test modules
synchronous and use an unlinked Server with `on_exit` cleanup:

```elixir
defmodule MyRedisIntegrationTest do
  use ExUnit.Case, async: false

  alias RedisServerWrapper.Server

  setup do
    port = 16_380
    {:ok, server} = Server.start(port: port, save: :disabled)

    on_exit(fn ->
      if Process.alive?(server), do: Server.stop(server)
    end)

    %{server: server, port: port}
  end

  test "round trips through Redis", %{server: server} do
    assert {:ok, "OK"} = Server.run(server, ["SET", "answer", "42"])
    assert {:ok, "42"} = Server.run(server, ["GET", "answer"])
  end
end
```

`Server.start/1` avoids linking the Redis owner to the individual test process.
An OTP-supervised fixture is also valid:

```elixir
child =
  Supervisor.child_spec(
    {RedisServerWrapper.Server, [port: 16_381, save: :disabled]},
    id: {:redis, 16_381}
  )

server = start_supervised!(child)
```

Use `Cluster.start/1` or `Sentinel.start/1` with the same `on_exit` pattern.
Give topology tests a larger ExUnit timeout because startup waits for actual
convergence:

```elixir
@tag timeout: 60_000
test "cluster behavior" do
  {:ok, cluster} =
    RedisServerWrapper.Cluster.start(
      masters: 3,
      replicas_per_master: 1,
      base_port: 17_000
    )

  on_exit(fn ->
    if Process.alive?(cluster), do: RedisServerWrapper.Cluster.stop(cluster)
  end)

  assert RedisServerWrapper.Cluster.healthy?(cluster)
end
```

## Parallel tests and ports

A Redis listener must own its port exclusively. The wrapper will not terminate
an existing listener to claim a port.

Prefer one of these strategies:

1. Run Redis-owning modules with `async: false` and assign fixed,
   non-overlapping port blocks.
2. Allocate blocks centrally with an Agent/ETS counter before tests start.
3. Ask the OS for an ephemeral port, close the reservation, and start Redis
   immediately. This has a small close/rebind race and is best for developer
   tests rather than hostile multi-tenant hosts.

```elixir
def free_tcp_port do
  {:ok, socket} =
    :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])

  {:ok, {_address, port}} = :inet.sockname(socket)
  :ok = :gen_tcp.close(socket)
  port
end
```

Port blocks must account for the whole topology:

- Server: every enabled plaintext or TLS listener.
- Cluster: `total_nodes = masters * (1 + replicas_per_master)` consecutive data
  ports beginning at `base_port`, plus one Cluster bus port at
  `data_port + 10_000` for every node. Both ranges must be free and at most
  65,535. The wrapper validates range overlap and overflow.
- Sentinel: `master_port`, `replicas` consecutive ports beginning at
  `replica_base_port`, and `sentinels` consecutive ports beginning at
  `sentinel_base_port`. These ranges may not overlap.

Cluster bus ports must also be reachable between nodes in a networked test
environment. Sentinel processes must reach `control_host:master_port`, and
replicas must reach the same concrete control host.

## Modules in CI

The repository's compatibility job builds
`test/support/version_matrix_module.c`, loads it through `:loadmodule`, and runs
the shipped `examples/module_loading.exs` on every supported Redis line.

To check your own module:

```bash
REDIS_MODULE=/absolute/path/to/module.so \
REDIS_MODULE_NAME=my_module \
  mix run examples/module_loading.exs
```

Optional load arguments can be supplied in shell syntax:

```bash
REDIS_MODULE=/absolute/path/to/module.so \
REDIS_MODULE_ARGS='stream-name "argument with spaces"' \
  mix run examples/module_loading.exs
```

For the event-stream demo, build `redis-event-stream-module` for the local
platform and Redis line, then run:

```bash
EVENT_STREAM_MODULE=/absolute/path/to/libredis_event_stream_module.so \
  mix run examples/event_stream.exs
```

## Troubleshooting

### Binary not found

Verify both tools resolve in the same environment as the BEAM:

```bash
redis-server --version
redis-cli --version
```

Otherwise pass `redis_server_bin:` and `redis_cli_bin:`. See the
[support policy](support.md) for Redis 8 Full and legacy Stack selection.

### Port or socket already in use

Choose another port/block and check system services. On macOS, AirPlay Receiver
commonly owns port 7000; Cluster defaults to 7100 for that reason. A Unix socket
path must not already exist.

### Startup or convergence timeout

Increase `timeout:` for process readiness and `convergence_timeout:` for
Cluster/Sentinel agreement. Errors retain the last observed health state.
Generated logs and configs live below the system temporary directory in
`redis-server-wrapper/node-*` while the instance exists.

For Cluster, verify both data and `+10_000` bus ports are available. For
Sentinel, verify every master, replica, and Sentinel range is non-overlapping.

### TLS failure

TLS requires a server certificate, key, and exactly one CA file or CA
directory. Redis requires client certificates by default; either supply
`tls_client_cert_file` and `tls_client_key_file`, or explicitly set
`tls_auth_clients: "no"`/`"optional"`.

### Module fails to load

Run the selected `redis-server` directly with the module to see its loader
error. Confirm the shared-library platform, architecture, Redis Module API, and
dependent libraries. A path that loads on Redis 7 or legacy Stack is not
automatically compatible with Redis 8.

### Leftover process after a hard test failure

Normal `Server.stop/1` cleanup sends `SHUTDOWN NOSAVE`, closes managed ports,
and verifies owned PIDs before a forced signal. A `:brutal_kill`, SIGKILL, OOM,
or VM crash can skip OTP termination callbacks. Use `managed: :forcola` for the
strongest owner-death cleanup, or stop an intentionally detached Manager
instance with `Manager.stop/1`.

### Missing `kill` or `lsof`

Managed startup and normal graceful shutdown still work without these tools.
Chaos signaling requires `kill`. Manager destructive cleanup may refuse to
proceed when `lsof` is unavailable because listener ownership cannot be
verified.
