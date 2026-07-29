defmodule RedisServerWrapper.Server do
  @moduledoc """
  GenServer managing a single redis-server process.

  Starts redis-server with a generated config file, tracks the OS PID,
  and sends SHUTDOWN NOSAVE on terminate (unless detached).

  ## Usage

      {:ok, pid} = RedisServerWrapper.Server.start_link(port: 6400, password: "secret")
      RedisServerWrapper.Server.ping(pid)
      RedisServerWrapper.Server.run(pid, ["SET", "key", "value"])
      RedisServerWrapper.Server.stop(pid)

  ## Options

  All options from `RedisServerWrapper.Config` are supported, plus:

    * `:redis_server_bin` - path to redis-server binary (default: "redis-server")
    * `:redis_cli_bin` - path to redis-cli binary (default: "redis-cli")
    * `:name` - GenServer name registration
    * `:timeout` - startup timeout in ms (default: 10_000)
    * `:loadmodule` - Redis modules to load; accepts module paths or
      `{path, [args]}` tuples (default: `[]`)
    * `:managed` - controls how the redis-server OS process is tied to the BEAM:
      * `true` (default) - redis-server runs as a Port tied to the BEAM lifecycle.
        When the BEAM exits, the port closes and redis-server receives SIGHUP.
        Teardown runs in `terminate/2`, which is skipped on a `:brutal_kill`
        supervisor shutdown or a hard BEAM death, so the OS process can be stranded.
      * `:forcola` - redis-server runs in the foreground under a `Forcola.Daemon`,
        which guarantees the OS process group is killed and confirmed dead on owner
        death or supervisor shutdown, including the paths where `terminate/2` never
        runs. Requires the optional `:forcola` dependency (`{:forcola, "~> 0.3"}`);
        `start_link` returns `{:error, :forcola_not_available}` if it is missing.
      * `false` - redis-server daemonizes independently (legacy behavior); the
        caller owns the OS process lifecycle.
  """

  use GenServer

  alias RedisServerWrapper.{Cli, Config, OSProcess}

  require Logger

  # Forcola is an optional dependency guarded by forcola_available?/0.
  @compile {:no_warn_undefined, Forcola.Daemon}

  @default_timeout 10_000

  defstruct [
    :config,
    :cli,
    :pid,
    :node_dir,
    :redis_server_bin,
    :port_ref,
    :daemon,
    managed: true,
    detached: false
  ]

  @type managed :: boolean() | :forcola

  @type t :: %__MODULE__{
          config: Config.t(),
          cli: Cli.t(),
          pid: non_neg_integer() | nil,
          node_dir: String.t() | nil,
          redis_server_bin: String.t(),
          port_ref: port() | nil,
          daemon: pid() | nil,
          managed: managed(),
          detached: boolean()
        }

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts and links a redis-server process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, server_opts} = extract_gen_opts(opts)
    GenServer.start_link(__MODULE__, server_opts, gen_opts)
  end

  @doc """
  Starts a redis-server process without linking.
  """
  @spec start(keyword()) :: GenServer.on_start()
  def start(opts \\ []) do
    {gen_opts, server_opts} = extract_gen_opts(opts)
    GenServer.start(__MODULE__, server_opts, gen_opts)
  end

  @doc "Pings the server. Returns true if alive."
  @spec ping(GenServer.server()) :: boolean()
  def ping(server), do: GenServer.call(server, :ping)

  @doc "Returns true if the redis-server OS process is still alive."
  @spec alive?(GenServer.server()) :: boolean()
  def alive?(server), do: GenServer.call(server, :alive?)

  @doc "Runs an arbitrary redis-cli command against this server."
  @spec run(GenServer.server(), [String.t()]) :: {:ok, String.t()} | {:error, String.t()}
  def run(server, args), do: GenServer.call(server, {:run, args})

  @doc "Returns connection info: host, port, password, pid, node_dir."
  @spec info(GenServer.server()) :: map()
  def info(server), do: GenServer.call(server, :info)

  @doc "Returns the Cli struct for direct use."
  @spec cli(GenServer.server()) :: Cli.t()
  def cli(server), do: GenServer.call(server, :cli)

  @doc """
  Detaches the server -- the redis-server OS process will NOT be stopped
  when this GenServer terminates. Returns `{:error, :managed_server}` in managed mode.
  """
  @spec detach(GenServer.server()) :: :ok | {:error, :managed_server}
  def detach(server), do: GenServer.call(server, :detach)

  @doc "Gracefully stops the GenServer (which stops redis-server unless detached)."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server, :normal)

  @doc """
  Returns the default redis-server binary path.
  Prefers the actual redis-server binary from redis-stack (includes modules)
  over the wrapper script, then falls back to plain redis-server.

  We avoid the redis-stack-server bash wrapper because it overrides our
  `dir` config with its own --dir flag, causing cluster config files to
  end up in the wrong place. Instead, we use the real binary directly.
  """
  @spec default_server_bin() :: String.t()
  def default_server_bin do
    # Prefer the actual binary inside the redis-stack cask (not the wrapper script)
    stack_bin = find_stack_redis_server()

    cond do
      stack_bin -> stack_bin
      System.find_executable("redis-server") -> "redis-server"
      true -> "redis-server"
    end
  end

  # Find the real redis-server binary inside the redis-stack installation.
  # The wrapper script at /opt/homebrew/bin/redis-stack-server just calls
  # the real binary with --loadmodule flags. We want the real binary so
  # we have full control over config (especially `dir`).
  defp find_stack_redis_server do
    paths = [
      "/opt/homebrew/Caskroom/redis-stack-server/*/bin/redis-server",
      "/usr/local/Caskroom/redis-stack-server/*/bin/redis-server"
    ]

    paths
    |> Enum.flat_map(&Path.wildcard/1)
    |> List.first()
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    redis_server_bin = Keyword.get_lazy(opts, :redis_server_bin, &default_server_bin/0)
    redis_cli_bin = Keyword.get(opts, :redis_cli_bin, "redis-cli")
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    managed = Keyword.get(opts, :managed, true)

    # Validate binaries and the managed backend selection
    with :ok <- validate_managed(managed),
         :ok <- check_binary(redis_server_bin),
         :ok <- check_binary(redis_cli_bin) do
      config_opts =
        Keyword.drop(opts, [:redis_server_bin, :redis_cli_bin, :name, :timeout, :managed])

      config = Config.new(config_opts)

      case start_redis_server(config, redis_server_bin, redis_cli_bin, timeout, managed) do
        {:ok, state} ->
          {:ok, state}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, Cli.ping(state.cli), state}
  end

  def handle_call(:alive?, _from, state) do
    {:reply, OSProcess.alive?(state.pid), state}
  end

  def handle_call({:run, args}, _from, state) do
    {:reply, Cli.run(state.cli, args), state}
  end

  def handle_call(:info, _from, state) do
    info = %{
      host: state.config.bind,
      port: state.config.port,
      password: state.config.password,
      pid: state.pid,
      node_dir: state.node_dir,
      detached: state.detached,
      managed: state.managed
    }

    {:reply, info, state}
  end

  def handle_call(:cli, _from, state) do
    {:reply, state.cli, state}
  end

  def handle_call(:detach, _from, %{managed: managed} = state)
      when managed in [true, :forcola] do
    Logger.warning(
      "Detaching a managed server is not supported; the OS process is tied to " <>
        "the BEAM lifecycle. Use managed: false to enable detach."
    )

    {:reply, {:error, :managed_server}, state}
  end

  def handle_call(:detach, _from, state) do
    {:reply, :ok, %{state | detached: true}}
  end

  @impl true
  def handle_info({:EXIT, daemon, reason}, %{daemon: daemon} = state) when is_pid(daemon) do
    # The Forcola.Daemon child exited (redis-server died on its own). Forcola
    # maps the child exit to :normal / {:exit_status, n} / {:exit_signal, n};
    # translate it into a GenServer stop so an OTP restart strategy behaves.
    Logger.info("Managed (forcola) redis-server exited: #{inspect(reason)}")
    {:stop, reason, %{state | daemon: nil, pid: nil}}
  end

  def handle_info({:EXIT, _port, _reason}, state) do
    # Ignore port exits from System.cmd calls (trap_exit catches these)
    {:noreply, state}
  end

  def handle_info({port_ref, {:exit_status, status}}, %{port_ref: port_ref} = state)
      when is_port(port_ref) do
    Logger.info("Managed redis-server exited with status #{status}")
    {:noreply, %{state | port_ref: nil, pid: nil}}
  end

  def handle_info({port_ref, {:data, {:eol, line}}}, %{port_ref: port_ref} = state)
      when is_port(port_ref) do
    Logger.debug("redis-server: #{line}")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{detached: true}) do
    Logger.debug("RedisServerWrapper.Server terminating (detached, not stopping redis-server)")
    :ok
  end

  def terminate(_reason, %{daemon: daemon} = state) when is_pid(daemon) do
    # Forcola.Daemon owns teardown: stopping it sends the shim a KILL and blocks
    # until the whole redis-server process group is confirmed dead (SIGTERM then
    # SIGKILL). No SHUTDOWN/Port.close/SIGKILL ladder needed on this path.
    Logger.debug(
      "RedisServerWrapper.Server terminating, stopping Forcola.Daemon for port #{state.config.port}"
    )

    stop_daemon(daemon)
    :ok
  end

  def terminate(_reason, state) do
    Logger.debug(
      "RedisServerWrapper.Server terminating, sending SHUTDOWN NOSAVE to port #{state.config.port}"
    )

    shutdown_if_owned(state)
    # Give it a moment to shut down
    Process.sleep(500)

    managed_pid_owned = managed_port_owns_pid?(state.port_ref, state.pid)

    # Close the port if managed
    if state.port_ref && Port.info(state.port_ref) != nil do
      Port.close(state.port_ref)
    end

    force_kill_pid =
      cond do
        managed_pid_owned -> state.pid
        is_nil(state.port_ref) and pid_owns_port?(state.pid, state.config.port) -> state.pid
        true -> nil
      end

    # Force kill only when ownership was re-established immediately before the
    # signal. Use the process group to also catch children of a custom wrapper.
    if force_kill_pid && OSProcess.alive?(force_kill_pid) do
      Logger.warning("redis-server PID #{state.pid} still alive after SHUTDOWN, sending SIGKILL")
      # Kill the process group (negative PID) to get wrapper + child
      warn_if_signal_unavailable(OSProcess.signal(-force_kill_pid, :kill), force_kill_pid)
      # Also try the individual PID in case process group kill didn't work
      warn_if_signal_unavailable(OSProcess.signal(force_kill_pid, :kill), force_kill_pid)
    end

    :ok
  end

  # -------------------------------------------------------------------
  # Internal helpers
  # -------------------------------------------------------------------

  defp start_redis_server(config, redis_server_bin, redis_cli_bin, timeout, managed) do
    case managed do
      :forcola -> start_managed_forcola(config, redis_server_bin, redis_cli_bin, timeout)
      true -> start_managed(config, redis_server_bin, redis_cli_bin, timeout)
      false -> start_unmanaged(config, redis_server_bin, redis_cli_bin, timeout)
    end
  end

  # Port-based: redis-server runs in the foreground, tied to the BEAM.
  defp start_managed(config, redis_server_bin, redis_cli_bin, timeout) do
    with :ok <- check_port_available(config.bind, config.port) do
      do_start_managed(config, redis_server_bin, redis_cli_bin, timeout)
    end
  end

  defp do_start_managed(config, redis_server_bin, redis_cli_bin, timeout) do
    node_dir = make_node_dir(config.port)

    config = %{
      config
      | daemonize: false,
        pidfile: Path.join(node_dir, "redis.pid"),
        dir: node_dir,
        logfile: config.logfile || Path.join(node_dir, "redis.log")
    }

    conf_path = Path.join(node_dir, "redis.conf")
    File.write!(conf_path, Config.to_config_string(config))

    server_bin_path = System.find_executable(redis_server_bin)

    # If using the redis-stack binary, load the Stack modules
    module_args = detect_stack_modules(server_bin_path)

    port_ref =
      Port.open({:spawn_executable, server_bin_path}, [
        {:args, [conf_path | module_args]},
        :binary,
        :exit_status,
        {:line, 1024}
      ])

    cli =
      Cli.new(
        bin: redis_cli_bin,
        host: config.bind,
        port: config.port,
        password: config.password
      )

    case Cli.wait_for_ready(cli, timeout) do
      :ok ->
        os_pid =
          case :erlang.port_info(port_ref, :os_pid) do
            {:os_pid, p} -> p
            _ -> read_pidfile(Path.join(node_dir, "redis.pid"))
          end

        state = %__MODULE__{
          config: config,
          cli: cli,
          pid: os_pid,
          node_dir: node_dir,
          redis_server_bin: redis_server_bin,
          port_ref: port_ref,
          managed: true
        }

        {:ok, state}

      {:error, :timeout} ->
        safe_port_close(port_ref)
        {:error, {:server_start_timeout, config.port}}

      {:error, {:unexpected_reply, reply}} ->
        safe_port_close(port_ref)
        {:error, {:port_in_use, config.port, reply}}
    end
  end

  # Forcola-managed: redis-server runs in the foreground under a Forcola.Daemon.
  # Same foreground contract as the Port path, but the daemon's Rust shim
  # guarantees the OS process group is killed and confirmed dead on owner death
  # or supervisor shutdown, including the :brutal_kill and hard-BEAM-death paths
  # where terminate/2 never runs.
  defp start_managed_forcola(config, redis_server_bin, redis_cli_bin, timeout) do
    if forcola_available?() do
      with :ok <- check_port_available(config.bind, config.port) do
        do_start_managed_forcola(config, redis_server_bin, redis_cli_bin, timeout)
      end
    else
      {:error, :forcola_not_available}
    end
  end

  defp do_start_managed_forcola(config, redis_server_bin, redis_cli_bin, timeout) do
    node_dir = make_node_dir(config.port)

    config = %{
      config
      | daemonize: false,
        pidfile: Path.join(node_dir, "redis.pid"),
        dir: node_dir,
        logfile: config.logfile || Path.join(node_dir, "redis.log")
    }

    conf_path = Path.join(node_dir, "redis.conf")
    File.write!(conf_path, Config.to_config_string(config))

    server_bin_path = System.find_executable(redis_server_bin)

    # If using the redis-stack binary, load the Stack modules
    module_args = detect_stack_modules(server_bin_path)

    cli =
      Cli.new(
        bin: redis_cli_bin,
        host: config.bind,
        port: config.port,
        password: config.password
      )

    daemon_opts = [
      argv: [server_bin_path, conf_path | module_args],
      ready: fn -> Cli.ping(cli) end,
      ready_timeout_ms: timeout,
      output: :logger,
      log_output: :debug,
      log_prefix: "redis-server: "
    ]

    case Forcola.Daemon.start_link(daemon_opts) do
      {:ok, daemon} ->
        state = %__MODULE__{
          config: config,
          cli: cli,
          pid: read_pidfile(Path.join(node_dir, "redis.pid")),
          node_dir: node_dir,
          redis_server_bin: redis_server_bin,
          daemon: daemon,
          managed: :forcola
        }

        {:ok, state}

      {:error, :ready_timeout} ->
        {:error, {:server_start_timeout, config.port}}

      {:error, {:exited_before_ready, reason}} ->
        {:error, {:server_start_failed, config.port, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp forcola_available?, do: Code.ensure_loaded?(Forcola.Daemon)

  defp validate_managed(managed) when managed in [true, false, :forcola], do: :ok
  defp validate_managed(other), do: {:error, {:invalid_managed, other}}

  # Daemonized: redis-server forks into background, independent of the BEAM.
  # No stale-process cleanup here -- in unmanaged mode the caller owns the
  # lifecycle (instances are intended to outlive this BEAM), and ppid=1 is
  # the normal state of any live detached redis-server, so a "kill anything
  # whose pid is in the pidfile" heuristic would silently murder live,
  # wanted instances. If the port is held, check_port_available returns
  # {:error, {:port_in_use, ...}} and the caller decides what to do.
  defp start_unmanaged(config, redis_server_bin, redis_cli_bin, timeout) do
    with :ok <- check_port_available(config.bind, config.port) do
      do_start_unmanaged(config, redis_server_bin, redis_cli_bin, timeout)
    end
  end

  defp do_start_unmanaged(config, redis_server_bin, redis_cli_bin, timeout) do
    node_dir = make_node_dir(config.port)
    pidfile_path = Path.join(node_dir, "redis.pid")

    config = %{
      config
      | daemonize: true,
        pidfile: pidfile_path,
        dir: node_dir,
        logfile: config.logfile || Path.join(node_dir, "redis.log")
    }

    conf_path = Path.join(node_dir, "redis.conf")
    File.write!(conf_path, Config.to_config_string(config))

    server_bin_path = System.find_executable(redis_server_bin) || redis_server_bin

    # If using the redis-stack binary, load the Stack modules
    module_args = detect_stack_modules(server_bin_path)

    case System.cmd(server_bin_path, [conf_path | module_args], stderr_to_stdout: true) do
      {_output, 0} ->
        cli =
          Cli.new(
            bin: redis_cli_bin,
            host: config.bind,
            port: config.port,
            password: config.password
          )

        case Cli.wait_for_ready(cli, timeout) do
          :ok ->
            pid = read_pidfile(pidfile_path)

            state = %__MODULE__{
              config: config,
              cli: cli,
              pid: pid,
              node_dir: node_dir,
              redis_server_bin: redis_server_bin,
              managed: false
            }

            {:ok, state}

          {:error, :timeout} ->
            {:error, {:server_start_timeout, config.port}}

          {:error, {:unexpected_reply, reply}} ->
            {:error, {:port_in_use, config.port, reply}}
        end

      {output, code} ->
        {:error, {:server_start_failed, config.port, code, output}}
    end
  end

  # Pre-flight: try to bind the target (bind, port) ourselves to detect the
  # common case where another process (another redis-server, macOS AirPlay,
  # a leftover daemon) already holds it. Catches the scenario before we
  # spawn redis-server and then mistake a foreign PING reply for success.
  # Still best-effort: there is an inherent TOCTOU race between closing our
  # probe socket and redis-server binding, and a probe on 127.0.0.1 does
  # not detect a wildcard-bound peer on the same port. Cli.wait_for_ready's
  # unexpected-reply handling is the backstop for those cases.
  defp check_port_available(bind, port) do
    case :inet.parse_address(to_charlist(bind)) do
      {:ok, ip} ->
        # Match Redis's address-reuse behavior so a recently closed client
        # connection does not make this preflight report a false :eaddrinuse.
        case :gen_tcp.listen(port, [
               :binary,
               {:ip, ip},
               {:active, false},
               {:reuseaddr, true}
             ]) do
          {:ok, sock} ->
            :gen_tcp.close(sock)
            :ok

          {:error, reason} ->
            {:error, {:port_in_use, port, reason}}
        end

      {:error, _} ->
        # Non-literal bind (e.g. hostname); skip the probe.
        :ok
    end
  end

  defp make_node_dir(port) do
    dir =
      Path.join([
        System.tmp_dir!(),
        "redis-server-wrapper",
        "node-#{port}"
      ])

    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    dir
  end

  defp read_pidfile(path) do
    case File.read(path) do
      {:ok, content} ->
        content |> String.trim() |> String.to_integer()

      {:error, _} ->
        nil
    end
  end

  # Stop the linked Forcola.Daemon, triggering its group-kill teardown. The
  # daemon may already be gone (redis-server exited on its own), so tolerate a
  # :noproc exit rather than crashing the terminating GenServer.
  defp stop_daemon(daemon) do
    if Process.alive?(daemon), do: GenServer.stop(daemon, :normal, :infinity)
  catch
    :exit, _ -> :ok
  end

  defp safe_port_close(port_ref) do
    if :erlang.port_info(port_ref) != :undefined do
      Port.close(port_ref)
    end
  rescue
    ArgumentError -> :ok
  end

  defp shutdown_if_owned(%{pid: nil}), do: :ok

  defp shutdown_if_owned(state) do
    if managed_port_owns_pid?(state.port_ref, state.pid) do
      run_shutdown(state)
    else
      shutdown_if_listener_owned(state, OSProcess.pids_on_port(state.config.port))
    end
  end

  defp shutdown_if_listener_owned(state, {:ok, pids}) do
    if state.pid in pids do
      run_shutdown(state)
    else
      Logger.warning(
        "Skipping Redis shutdown on port #{state.config.port}: " <>
          "tracked PID #{state.pid} no longer owns the listener"
      )
    end
  end

  defp shutdown_if_listener_owned(state, {:error, {:executable_not_found, "lsof"}}) do
    Logger.warning(
      "lsof is unavailable; skipping ownership-sensitive Redis shutdown " <>
        "on port #{state.config.port}"
    )
  end

  defp run_shutdown(state) do
    _result = Cli.run(state.cli, ["SHUTDOWN", "NOSAVE"])
    :ok
  end

  defp managed_port_owns_pid?(port_ref, pid) when is_port(port_ref) and is_integer(pid) do
    case :erlang.port_info(port_ref, :os_pid) do
      {:os_pid, ^pid} -> true
      _other -> false
    end
  end

  defp managed_port_owns_pid?(_port_ref, _pid), do: false

  defp pid_owns_port?(pid, port) when is_integer(pid) do
    case OSProcess.pids_on_port(port) do
      {:ok, pids} -> pid in pids
      {:error, _reason} -> false
    end
  end

  defp pid_owns_port?(_pid, _port), do: false

  defp warn_if_signal_unavailable(
         {:error, {:executable_not_found, "kill"}},
         pid
       ) do
    Logger.warning("kill is unavailable; unable to signal Redis process #{pid}")
  end

  defp warn_if_signal_unavailable(_result, _pid), do: :ok

  # Detect Redis Stack modules (RedisJSON, RediSearch, etc.) if we're using
  # the redis-stack binary. Returns command-line args like
  # ["--loadmodule", "/path/to/rejson.so", "--loadmodule", "/path/to/redisearch.so", ...]
  defp detect_stack_modules(server_bin_path) do
    # Check if this binary lives inside a redis-stack installation
    bin_dir = Path.dirname(server_bin_path)
    lib_dir = Path.join(Path.dirname(bin_dir), "lib")

    if File.dir?(lib_dir) do
      # Load modules in a sensible order
      modules = [
        {"rediscompat.so", []},
        {"redisearch.so", ["MAXSEARCHRESULTS", "10000", "MAXAGGREGATERESULTS", "10000"]},
        {"redistimeseries.so", []},
        {"rejson.so", []},
        {"redisbloom.so", []}
      ]

      modules
      |> Enum.flat_map(&module_args(lib_dir, &1))
    else
      []
    end
  end

  defp module_args(lib_dir, {mod_file, args}) do
    mod_path = Path.join(lib_dir, mod_file)
    if File.exists?(mod_path), do: ["--loadmodule", mod_path | args], else: []
  end

  defp check_binary(bin) do
    case System.find_executable(bin) do
      nil -> {:error, {:binary_not_found, bin}}
      _path -> :ok
    end
  end

  defp extract_gen_opts(opts) do
    case Keyword.pop(opts, :name) do
      {nil, rest} -> {[], rest}
      {name, rest} -> {[name: name], rest}
    end
  end
end
