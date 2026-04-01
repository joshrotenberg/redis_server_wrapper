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
    detached: false
  ]

  @type t :: %__MODULE__{
          config: Config.t(),
          cli: Cli.t(),
          pid: non_neg_integer() | nil,
          node_dir: String.t() | nil,
          redis_server_bin: String.t(),
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
  Detaches the server — the redis-server OS process will NOT be stopped
  when this GenServer terminates.
  """
  @spec detach(GenServer.server()) :: :ok
  def detach(server), do: GenServer.call(server, :detach)

  @doc "Gracefully stops the GenServer (which stops redis-server unless detached)."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server, :normal)

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    redis_server_bin = Keyword.get(opts, :redis_server_bin, "redis-server")
    redis_cli_bin = Keyword.get(opts, :redis_cli_bin, "redis-cli")
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    # Validate binaries exist
    with :ok <- check_binary(redis_server_bin),
         :ok <- check_binary(redis_cli_bin) do
      config_opts =
        Keyword.drop(opts, [:redis_server_bin, :redis_cli_bin, :name, :timeout])

      config = Config.new(config_opts)

      case start_redis_server(config, redis_server_bin, redis_cli_bin, timeout) do
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
      detached: state.detached
    }

    {:reply, info, state}
  end

  def handle_call(:cli, _from, state) do
    {:reply, state.cli, state}
  end

  def handle_call(:detach, _from, state) do
    {:reply, :ok, %{state | detached: true}}
  end

  @impl true
  def handle_info({:EXIT, _port, _reason}, state) do
    # Ignore port exits from System.cmd calls (trap_exit catches these)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{detached: true}) do
    Logger.debug("RedisServerWrapper.Server terminating (detached, not stopping redis-server)")
    :ok
  end

  def terminate(_reason, state) do
    Logger.debug("RedisServerWrapper.Server terminating, sending SHUTDOWN NOSAVE to port #{state.config.port}")
    Cli.shutdown(state.cli)
    # Give it a moment to shut down
    Process.sleep(500)

    # Force kill if still alive
    if state.pid && pid_alive?(state.pid) do
      Logger.warning("redis-server PID #{state.pid} still alive after SHUTDOWN, sending SIGKILL")
      System.cmd("kill", ["-9", to_string(state.pid)], stderr_to_stdout: true)
    end

    :ok
  end

  # -------------------------------------------------------------------
  # Internal helpers
  # -------------------------------------------------------------------

  defp start_redis_server(config, redis_server_bin, redis_cli_bin, timeout) do
    node_dir = make_node_dir(config.port)

    # Inject daemonize + pidfile + dir into config
    config = %{
      config
      | daemonize: true,
        pidfile: Path.join(node_dir, "redis.pid"),
        dir: node_dir,
        logfile: config.logfile || Path.join(node_dir, "redis.log")
    }

    # Write redis.conf
    conf_path = Path.join(node_dir, "redis.conf")
    File.write!(conf_path, Config.to_config_string(config))

    # Start redis-server
    case System.cmd(redis_server_bin, [conf_path], stderr_to_stdout: true) do
      {_output, 0} ->
        # Read PID from pidfile
        cli = Cli.new(
          bin: redis_cli_bin,
          host: config.bind,
          port: config.port,
          password: config.password
        )

        case Cli.wait_for_ready(cli, timeout) do
          :ok ->
            pid = read_pidfile(Path.join(node_dir, "redis.pid"))

            state = %__MODULE__{
              config: config,
              cli: cli,
              pid: pid,
              node_dir: node_dir,
              redis_server_bin: redis_server_bin
            }

            {:ok, state}

          {:error, :timeout} ->
            {:error, {:server_start_timeout, config.port}}
        end

      {output, code} ->
        {:error, {:server_start_failed, config.port, code, output}}
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
