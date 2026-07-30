# Security and data safety

RedisServerWrapper starts real operating-system processes and writes real Redis
configuration. Treat its generated files and Manager state as credentials, and
treat stop/cleanup/chaos operations as production-inappropriate unless that is
explicitly your intent.

## Credentials and generated files

Passwords, ACL definitions, replication credentials, TLS key paths, and raw
directives are written to generated Redis or Sentinel configuration files.
Server and Sentinel configuration directories are created with mode `0700`;
configuration, PID, and Manager state files use `0600`. Sentinel is launched
under a private umask because Redis rewrites `sentinel.conf` as topology state
changes.

The Manager stores connection data and plaintext passwords in
`~/.config/redis-server-wrapper/instances.json` by default. Its console output
redacts credentials, but `Manager.credentials/1`, `Server.info/1`, and
connection structs intentionally return them to the caller. Do not log or
serialize those values without applying your own redaction.

Manager writes are atomic and serialized across connected BEAM nodes.
Independent, unconnected BEAM instances must not share one state file. A custom
path can be configured:

```elixir
config :redis_server_wrapper,
  manager_state_file: "/private/path/redis-server-wrapper.json"
```

The parent directory must already have an appropriate trust boundary if it is
not the default path.

## Process arguments and environment

Redis is launched with the generated configuration path, rather than with
passwords on its command line. `redis-cli` authentication uses the
`REDISCLI_AUTH` child-process environment variable instead of `-a`, keeping the
password out of the argument vector. Environment variables may still be
observable to privileged users on some systems.

Module paths and legacy Stack module discovery may appear in the Redis argument
vector. Do not use module paths or load arguments as a secret-storage channel;
load arguments are also written to `redis.conf`.

## Network exposure

The default bind address is loopback-only (`127.0.0.1`). If you bind to
`0.0.0.0`, `::`, or another non-loopback address:

- configure a password or named ACL user;
- apply host/network firewall rules;
- use TLS for untrusted networks;
- set `control_host` to one concrete reachable address;
- review Redis protected-mode and module exposure for your environment.

`tls_insecure: true` disables server-certificate verification for lifecycle
commands. It is intended only for controlled tests. For mutual TLS, keep client
private keys readable only by the test account.

## Stop, cleanup, and chaos semantics

`Server.stop/1`, `Cluster.stop/1`, and `Sentinel.stop/1` send `SHUTDOWN NOSAVE`.
Unsaved data is deliberately discarded. The Manager's `stop/1` and
`stop_all/0` do the same for detached instances.

Before signaling a Manager-owned process, the Manager uses listener ownership
to match the currently bound process against its recorded PID. If ownership
cannot be verified, it refuses destructive cleanup rather than signaling an
unknown process. `Manager.cleanup/0` only removes state entries that appear
stopped; it does not stop live instances.

The `RedisServerWrapper.Chaos` API is intentionally destructive:

- `kill_node/1` and `kill_master/2` use SIGKILL;
- `flushall/1` deletes every key;
- `fill_memory/2` writes data and can trigger eviction or OOM behavior;
- partition/freeze operations can make a topology unavailable.

Use isolated ports and disposable data directories. Always recover frozen PIDs
in `on_exit`/`after` cleanup, and propagate the structured errors returned by
Chaos rather than assuming a signal was delivered.

The default managed Port backend can strand a child after a hard BEAM death or
`:brutal_kill`, because OTP does not run `terminate/2` on those paths. Use
`managed: :forcola` when child cleanup must survive those termination modes.
