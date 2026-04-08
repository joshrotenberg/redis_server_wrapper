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
    * `:managed` - when `true` (default), redis-server runs as a Port tied to the
      BEAM lifecycle. When the BEAM exits, the port closes and redis-server receives
      SIGHUP. When `false`, redis-server daemonizes independently (legacy behavior).
  """

  use GenServer

  alias RedisServerWrapper.{Cli, Config}

  require Logger

  @default_timeout 10_000

  defstruct [
    :config,
    :cli,
    :pid,
    :node_dir,
    :redis_server_bin,
    :port_ref,
    managed: true,
    detached: false
  ]

  @type t :: %__MODULE__{
          config: Config.t(),
          cli: Cli.t(),
          pid: non_neg_integer() | nil,
          node_dir: String.t() | nil,
          redis_server_bin: String.t(),
          port_ref: port() | nil,
          managed: boolean(),
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

  @spec default_server_bin() :: String.t()
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

    # Validate binaries exist
    with :ok <- check_binary(redis_server_bin),
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
    {:reply, pid_alive?(state.pid), state}
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

  def handle_call(:detach, _from, %{managed: true} = state) do
    Logger.warning(
      "Detaching a managed (Port-based) server is not supported; " <>
        "the OS process is tied to the BEAM lifecycle. Use managed: false to enable detach."
    )

    {:reply, {:error, :managed_server}, state}
  end

  def handle_call(:detach, _from, state) do
    {:reply, :ok, %{state | detached: true}}
  end

  @impl true
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

  def terminate(_reason, state) do
    Logger.debug(
      "RedisServerWrapper.Server terminating, sending SHUTDOWN NOSAVE to port #{state.config.port}"
    )

    Cli.shutdown(state.cli)
    # Give it a moment to shut down
    Process.sleep(500)

    # Close the port if managed
    if state.port_ref && Port.info(state.port_ref) != nil do
      Port.close(state.port_ref)
    end

    # Force kill if still alive.
    # Use kill on the process group (-pid) to also catch child processes.
    # This handles redis-stack-server (bash wrapper) which spawns a child redis-server.
    if state.pid && pid_alive?(state.pid) do
      Logger.warning("redis-server PID #{state.pid} still alive after SHUTDOWN, sending SIGKILL")
      # Kill the process group (negative PID) to get wrapper + child
      System.cmd("kill", ["-9", "-#{state.pid}"], stderr_to_stdout: true)
      # Also try the individual PID in case process group kill didn't work
      System.cmd("kill", ["-9", to_string(state.pid)], stderr_to_stdout: true)
    end

    # Extra safety: find and kill any redis-server on our port
    kill_by_port(state.config.port)

    :ok
  end

  # -------------------------------------------------------------------
  # Internal helpers
  # -------------------------------------------------------------------

  defp start_redis_server(config, redis_server_bin, redis_cli_bin, timeout, managed) do
    if managed do
      start_managed(config, redis_server_bin, redis_cli_bin, timeout)
    else
      start_unmanaged(config, redis_server_bin, redis_cli_bin, timeout)
    end
  end

  # Port-based: redis-server runs in the foreground, tied to the BEAM.
  defp start_managed(config, redis_server_bin, redis_cli_bin, timeout) do
    # Check for stale process from a previous (possibly daemonized) run
    stale_pidfile =
      Path.join([System.tmp_dir!(), "redis-server-wrapper", "node-#{config.port}", "redis.pid"])

    kill_stale_process(stale_pidfile)

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
    end
  end

  # Daemonized: redis-server forks into background, independent of the BEAM.
  defp start_unmanaged(config, redis_server_bin, redis_cli_bin, timeout) do
    # Check for stale process from a previous run before wiping the node dir
    stale_pidfile =
      Path.join([System.tmp_dir!(), "redis-server-wrapper", "node-#{config.port}", "redis.pid"])

    kill_stale_process(stale_pidfile)

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

    case System.cmd(redis_server_bin, [conf_path], stderr_to_stdout: true) do
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
        end

      {output, code} ->
        {:error, {:server_start_failed, config.port, code, output}}
    end
  end

  defp kill_stale_process(pidfile_path) do
    pidfile_path
    |> read_pidfile()
    |> maybe_kill_stale()
  end

  defp maybe_kill_stale(nil), do: :ok

  defp maybe_kill_stale(stale_pid) do
    if pid_alive?(stale_pid) do
      Logger.warning("Killing stale redis-server process #{stale_pid}")
      System.cmd("kill", [to_string(stale_pid)], stderr_to_stdout: true)
      Process.sleep(500)
      force_kill_if_alive(stale_pid)
    end
  end

  defp force_kill_if_alive(pid) do
    if pid_alive?(pid) do
      Logger.warning("Stale PID #{pid} still alive, sending SIGKILL")
      System.cmd("kill", ["-9", to_string(pid)], stderr_to_stdout: true)
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

  defp pid_alive?(nil), do: false

  defp pid_alive?(pid) do
    case System.cmd("kill", ["-0", to_string(pid)], stderr_to_stdout: true) do
      {_, 0} -> true
      _ -> false
    end
  end

  defp safe_port_close(port_ref) do
    if :erlang.port_info(port_ref) != :undefined do
      Port.close(port_ref)
    end
  rescue
    ArgumentError -> :ok
  end

  # Kill any redis-server process listening on a specific port.
  # This handles orphaned processes from wrapper scripts (redis-stack-server).
  defp kill_by_port(port) when is_integer(port) do
    case System.cmd("lsof", ["-ti", ":#{port}"], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.split("\n", trim: true)
        |> Enum.each(fn pid_str ->
          System.cmd("kill", ["-9", String.trim(pid_str)], stderr_to_stdout: true)
        end)

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp kill_by_port(_), do: :ok

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
