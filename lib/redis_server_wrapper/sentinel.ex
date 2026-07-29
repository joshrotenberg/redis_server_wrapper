defmodule RedisServerWrapper.Sentinel do
  @moduledoc """
  GenServer managing a Redis Sentinel topology: master + replicas + sentinel processes.

  ## Usage

      {:ok, pid} = RedisServerWrapper.Sentinel.start_link(
        master_port: 6390,
        replicas: 2,
        sentinels: 3
      )

      RedisServerWrapper.Sentinel.healthy?(pid)
      RedisServerWrapper.Sentinel.master_addr(pid)
      RedisServerWrapper.Sentinel.stop(pid)

  ## Options

    * `:master_name` - sentinel master name (default: "mymaster")
    * `:master_port` - master port (default: 6390)
    * `:replicas` - number of replica nodes (default: 2)
    * `:replica_base_port` - starting port for replicas (default: master_port + 1)
    * `:sentinels` - number of sentinel processes (default: 3)
    * `:sentinel_base_port` - starting port for sentinels (default: 26389)
    * `:quorum` - sentinel quorum (default: 2)
    * `:down_after_ms` - down-after-milliseconds (default: 5000)
    * `:failover_timeout_ms` - failover timeout (default: 10_000)
    * `:bind` - bind address (default: "127.0.0.1")
    * `:password` - Redis password
    * `:redis_server_bin` - redis-server binary path
    * `:redis_cli_bin` - redis-cli binary path
    * `:timeout` - startup timeout per node in ms (default: 10_000)
    * `:loadmodule` - modules loaded into the master and every replica; accepts
      paths or `{path, [args]}` tuples (default: `[]`). Sentinel processes do
      not load data modules.
    * `:managed` - process lifecycle backend forwarded to the master and every
      replica. See `RedisServerWrapper.Server` for `true`, `:forcola`, and
      `false`. Only `managed: false` supports `detach/1`. Sentinel processes
      always daemonize regardless of this flag.
  """

  use GenServer

  alias RedisServerWrapper.{Cli, OSProcess, Server}

  require Logger

  defstruct [
    :master_name,
    :master_port,
    :bind,
    :password,
    :redis_cli_bin,
    :master_pid,
    :num_replicas,
    :num_sentinels,
    replica_pids: [],
    sentinel_os_pids: [],
    sentinel_ports: [],
    sentinel_dir: nil,
    detached: false
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, sentinel_opts} = extract_gen_opts(opts)
    GenServer.start_link(__MODULE__, sentinel_opts, gen_opts)
  end

  @spec start(keyword()) :: GenServer.on_start()
  def start(opts \\ []) do
    {gen_opts, sentinel_opts} = extract_gen_opts(opts)
    GenServer.start(__MODULE__, sentinel_opts, gen_opts)
  end

  @doc "Returns the master address."
  @spec master_addr(GenServer.server()) :: String.t()
  def master_addr(server), do: GenServer.call(server, :master_addr)

  @doc "Returns all sentinel addresses."
  @spec sentinel_addrs(GenServer.server()) :: [String.t()]
  def sentinel_addrs(server), do: GenServer.call(server, :sentinel_addrs)

  @doc "Returns info about the topology."
  @spec info(GenServer.server()) :: map()
  def info(server), do: GenServer.call(server, :info)

  @doc "Checks sentinel health: master reachable, expected replicas and sentinels."
  @spec healthy?(GenServer.server()) :: boolean()
  def healthy?(server), do: GenServer.call(server, :healthy?, 15_000)

  @doc "Queries SENTINEL MASTER for the given master name."
  @spec poke(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def poke(server), do: GenServer.call(server, :poke)

  @doc "Detaches daemonized data nodes so their OS processes survive this GenServer."
  @spec detach(GenServer.server()) :: :ok | {:error, :managed_server}
  def detach(server), do: GenServer.call(server, :detach)

  @doc "Stops the sentinel topology."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server, :normal)

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    master_name = Keyword.get(opts, :master_name, "mymaster")
    master_port = Keyword.get(opts, :master_port, 6390)
    num_replicas = Keyword.get(opts, :replicas, 2)

    replica_base_port =
      case Keyword.fetch(opts, :replica_base_port) do
        {:ok, port} -> port
        :error when is_integer(master_port) -> master_port + 1
        :error -> master_port
      end

    num_sentinels = Keyword.get(opts, :sentinels, 3)
    sentinel_base_port = Keyword.get(opts, :sentinel_base_port, 26_389)
    quorum = Keyword.get(opts, :quorum, 2)
    down_after_ms = Keyword.get(opts, :down_after_ms, 5000)
    failover_timeout_ms = Keyword.get(opts, :failover_timeout_ms, 10_000)
    bind = Keyword.get(opts, :bind, "127.0.0.1")
    password = Keyword.get(opts, :password)

    redis_server_bin =
      Keyword.get_lazy(opts, :redis_server_bin, &RedisServerWrapper.Server.default_server_bin/0)

    redis_cli_bin = Keyword.get(opts, :redis_cli_bin, "redis-cli")
    timeout = Keyword.get(opts, :timeout, 10_000)
    loadmodule = Keyword.get(opts, :loadmodule, [])
    managed = Keyword.get(opts, :managed, true)

    node_opts = %{
      bind: bind,
      password: password,
      redis_server_bin: redis_server_bin,
      redis_cli_bin: redis_cli_bin,
      timeout: timeout,
      loadmodule: loadmodule,
      managed: managed
    }

    validation_settings = %{
      master_port: master_port,
      replicas: num_replicas,
      replica_base_port: replica_base_port,
      sentinels: num_sentinels,
      sentinel_base_port: sentinel_base_port,
      quorum: quorum,
      down_after_ms: down_after_ms,
      failover_timeout_ms: failover_timeout_ms,
      timeout: timeout
    }

    with :ok <- validate_options(validation_settings),
         {:ok, master_pid} <- start_master(master_port, node_opts),
         {:ok, replica_pids} <-
           start_replicas(num_replicas, replica_base_port, master_port, node_opts),
         # Let replication link up
         _ <- Process.sleep(1000),
         {:ok, sentinel_os_pids, sentinel_dir} <-
           start_sentinels(%{
             count: num_sentinels,
             base_port: sentinel_base_port,
             master_name: master_name,
             master_port: master_port,
             bind: bind,
             password: password,
             quorum: quorum,
             down_after_ms: down_after_ms,
             failover_timeout_ms: failover_timeout_ms,
             redis_server_bin: redis_server_bin,
             redis_cli_bin: redis_cli_bin,
             timeout: timeout
           }) do
      # Wait for sentinel discovery
      Process.sleep(2000)

      sentinel_ports = ports_from(sentinel_base_port, num_sentinels)

      state = %__MODULE__{
        master_name: master_name,
        master_port: master_port,
        bind: bind,
        password: password,
        redis_cli_bin: redis_cli_bin,
        master_pid: master_pid,
        num_replicas: num_replicas,
        num_sentinels: num_sentinels,
        replica_pids: replica_pids,
        sentinel_os_pids: sentinel_os_pids,
        sentinel_ports: sentinel_ports,
        sentinel_dir: sentinel_dir
      }

      {:ok, state}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:master_addr, _from, state) do
    {:reply, "#{state.bind}:#{state.master_port}", state}
  end

  def handle_call(:sentinel_addrs, _from, state) do
    addrs = Enum.map(state.sentinel_ports, &"#{state.bind}:#{&1}")
    {:reply, addrs, state}
  end

  def handle_call(:info, _from, state) do
    info = %{
      master_name: state.master_name,
      master_addr: "#{state.bind}:#{state.master_port}",
      replicas: state.num_replicas,
      sentinels: state.num_sentinels,
      sentinel_addrs: Enum.map(state.sentinel_ports, &"#{state.bind}:#{&1}")
    }

    {:reply, info, state}
  end

  def handle_call(:healthy?, _from, state) do
    result =
      Enum.any?(state.sentinel_ports, fn port ->
        cli = Cli.new(bin: state.redis_cli_bin, host: state.bind, port: port)

        case Cli.sentinel_master(cli, state.master_name) do
          {:ok, info} ->
            flags = Map.get(info, "flags", "")
            num_slaves = String.to_integer(Map.get(info, "num-slaves", "0"))
            num_sentinels = String.to_integer(Map.get(info, "num-other-sentinels", "0")) + 1

            flags == "master" &&
              num_slaves >= state.num_replicas &&
              num_sentinels >= state.num_sentinels

          _ ->
            false
        end
      end)

    {:reply, result, state}
  end

  def handle_call(:poke, _from, state) do
    result =
      Enum.find_value(state.sentinel_ports, {:error, :no_reachable_sentinel}, fn port ->
        cli = Cli.new(bin: state.redis_cli_bin, host: state.bind, port: port)

        case Cli.sentinel_master(cli, state.master_name) do
          {:ok, info} -> {:ok, info}
          _ -> nil
        end
      end)

    {:reply, result, state}
  end

  def handle_call(:detach, _from, state) do
    case detach_servers([state.master_pid | state.replica_pids]) do
      :ok -> {:reply, :ok, %{state | detached: true}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    if reason != :normal do
      Logger.warning("Sentinel topology process #{inspect(pid)} exited: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{detached: true} = state) do
    Logger.debug("RedisServerWrapper.Sentinel terminating (OS processes detached)")
    stop_servers(state.replica_pids ++ [state.master_pid])
    :ok
  end

  def terminate(_reason, state) do
    Logger.debug("RedisServerWrapper.Sentinel terminating, stopping topology")

    # Stop sentinels first (they're raw OS processes, not GenServers)
    Enum.each(state.sentinel_os_pids, fn pid ->
      OSProcess.signal(pid, :term)
    end)

    Process.sleep(500)

    # Force kill any remaining sentinel processes
    Enum.each(state.sentinel_os_pids, fn pid ->
      if OSProcess.alive?(pid), do: OSProcess.signal(pid, :kill)
    end)

    # Stop replicas, then master
    stop_servers(state.replica_pids ++ [state.master_pid])

    # Clean up sentinel config directory
    if state.sentinel_dir, do: File.rm_rf(state.sentinel_dir)

    :ok
  end

  # -------------------------------------------------------------------
  # Internal
  # -------------------------------------------------------------------

  defp start_master(port, node_opts) do
    Server.start_link(
      port: port,
      bind: node_opts.bind,
      password: node_opts.password,
      redis_server_bin: node_opts.redis_server_bin,
      redis_cli_bin: node_opts.redis_cli_bin,
      timeout: node_opts.timeout,
      managed: node_opts.managed,
      loadmodule: node_opts.loadmodule,
      save: :disabled
    )
  end

  defp start_replicas(0, _base_port, _master_port, _node_opts), do: {:ok, []}

  defp start_replicas(count, base_port, master_port, node_opts) do
    results =
      Enum.reduce_while(0..(count - 1), {:ok, []}, fn i, {:ok, acc} ->
        port = base_port + i

        opts = [
          port: port,
          bind: node_opts.bind,
          password: node_opts.password,
          masterauth: node_opts.password,
          replicaof: {node_opts.bind, master_port},
          redis_server_bin: node_opts.redis_server_bin,
          redis_cli_bin: node_opts.redis_cli_bin,
          timeout: node_opts.timeout,
          managed: node_opts.managed,
          loadmodule: node_opts.loadmodule,
          save: :disabled
        ]

        case Server.start_link(opts) do
          {:ok, pid} ->
            {:cont, {:ok, acc ++ [pid]}}

          {:error, reason} ->
            Enum.each(acc, &Server.stop/1)
            {:halt, {:error, {:replica_start_failed, port, reason}}}
        end
      end)

    results
  end

  defp start_sentinels(opts) do
    %{count: count, base_port: base_port} = opts

    sentinel_dir =
      Path.join([
        System.tmp_dir!(),
        "redis-server-wrapper",
        "sentinel-#{System.system_time(:nanosecond)}"
      ])

    File.mkdir_p!(sentinel_dir)

    results =
      Enum.reduce_while(0..(count - 1), {:ok, []}, fn i, {:ok, acc} ->
        port = base_port + i

        case start_single_sentinel(opts, sentinel_dir, port) do
          {:ok, os_pid} ->
            {:cont, {:ok, acc ++ [os_pid]}}

          {:error, reason} ->
            kill_pids(acc)
            {:halt, {:error, {:sentinel_start_failed, port, reason}}}
        end
      end)

    case results do
      {:ok, pids} -> {:ok, pids, sentinel_dir}
      error -> error
    end
  end

  defp start_single_sentinel(opts, sentinel_dir, port) do
    node_dir = Path.join(sentinel_dir, "sentinel-#{port}")
    File.mkdir_p!(node_dir)

    conf_content = generate_sentinel_conf(opts, node_dir, port)
    conf_path = Path.join(node_dir, "sentinel.conf")
    File.write!(conf_path, conf_content)

    start_sentinel_process(
      opts.redis_server_bin,
      conf_path,
      node_dir,
      opts.redis_cli_bin,
      opts.bind,
      port,
      opts.timeout
    )
  end

  defp generate_sentinel_conf(opts, dir, port) do
    %{
      bind: bind,
      master_name: master_name,
      master_port: master_port,
      password: password,
      quorum: quorum,
      down_after_ms: down_after_ms,
      failover_timeout_ms: failover_timeout_ms
    } = opts

    lines = [
      "port #{port}",
      "bind #{bind}",
      "daemonize yes",
      "pidfile #{Path.join(dir, "sentinel.pid")}",
      "logfile #{Path.join(dir, "sentinel.log")}",
      "dir #{dir}",
      "sentinel monitor #{master_name} #{bind} #{master_port} #{quorum}",
      "sentinel down-after-milliseconds #{master_name} #{down_after_ms}",
      "sentinel failover-timeout #{master_name} #{failover_timeout_ms}",
      "sentinel parallel-syncs #{master_name} 1"
    ]

    lines =
      if password do
        lines ++ ["sentinel auth-pass #{master_name} #{password}"]
      else
        lines
      end

    Enum.join(lines, "\n") <> "\n"
  end

  defp kill_pids(pids) do
    Enum.each(pids, fn pid ->
      OSProcess.signal(pid, :term)
    end)
  end

  defp start_sentinel_process(
         redis_server_bin,
         conf_path,
         node_dir,
         redis_cli_bin,
         bind,
         port,
         timeout
       ) do
    case System.cmd(redis_server_bin, [conf_path, "--sentinel"], stderr_to_stdout: true) do
      {_output, 0} ->
        # Wait for sentinel to be ready
        cli = Cli.new(bin: redis_cli_bin, host: bind, port: port)

        case Cli.wait_for_ready(cli, timeout) do
          :ok ->
            pid_path = Path.join(node_dir, "sentinel.pid")
            pid = read_pidfile(pid_path)
            {:ok, pid}

          {:error, :timeout} ->
            {:error, {:sentinel_start_timeout, port}}

          {:error, {:unexpected_reply, reply}} ->
            {:error, {:sentinel_port_in_use, port, reply}}
        end

      {output, code} ->
        {:error, {:sentinel_start_failed, port, code, output}}
    end
  end

  defp read_pidfile(path) do
    case File.read(path) do
      {:ok, content} -> content |> String.trim() |> String.to_integer()
      {:error, _} -> nil
    end
  end

  defp validate_options(settings) do
    with :ok <- valid_port(:master_port, settings.master_port),
         :ok <- non_negative_integer(:replicas, settings.replicas),
         :ok <- positive_integer(:sentinels, settings.sentinels),
         :ok <- valid_port(:sentinel_base_port, settings.sentinel_base_port),
         :ok <- positive_integer(:quorum, settings.quorum),
         :ok <- quorum_within_sentinel_count(settings.quorum, settings.sentinels),
         :ok <- positive_integer(:down_after_ms, settings.down_after_ms),
         :ok <- positive_integer(:failover_timeout_ms, settings.failover_timeout_ms),
         :ok <- positive_integer(:timeout, settings.timeout),
         :ok <-
           valid_port_span(:replicas, settings.replica_base_port, settings.replicas),
         :ok <-
           valid_port_span(:sentinels, settings.sentinel_base_port, settings.sentinels) do
      all_ports =
        [settings.master_port] ++
          ports_from(settings.replica_base_port, settings.replicas) ++
          ports_from(settings.sentinel_base_port, settings.sentinels)

      if MapSet.size(MapSet.new(all_ports)) == length(all_ports) do
        :ok
      else
        {:error, {:overlapping_ports, all_ports}}
      end
    end
  end

  defp positive_integer(_key, value) when is_integer(value) and value > 0, do: :ok
  defp positive_integer(key, value), do: {:error, {:invalid_option, key, value}}

  defp non_negative_integer(_key, value) when is_integer(value) and value >= 0, do: :ok
  defp non_negative_integer(key, value), do: {:error, {:invalid_option, key, value}}

  defp valid_port(_key, value) when is_integer(value) and value in 1..65_535, do: :ok
  defp valid_port(key, value), do: {:error, {:invalid_option, key, value}}

  defp quorum_within_sentinel_count(quorum, sentinels) when quorum <= sentinels, do: :ok

  defp quorum_within_sentinel_count(quorum, sentinels),
    do: {:error, {:invalid_quorum, quorum, sentinels}}

  defp valid_port_span(_key, _base_port, 0), do: :ok

  defp valid_port_span(key, base_port, count)
       when is_integer(base_port) and base_port in 1..65_535 do
    last_port = base_port + count - 1

    if last_port <= 65_535 do
      :ok
    else
      {:error, {:invalid_port_range, key, base_port, last_port}}
    end
  end

  defp valid_port_span(key, base_port, _count),
    do: {:error, {:invalid_option, :"#{key}_base_port", base_port}}

  defp ports_from(_base_port, 0), do: []
  defp ports_from(base_port, count), do: Enum.map(0..(count - 1), &(base_port + &1))

  defp detach_servers(servers) do
    Enum.reduce_while(servers, :ok, fn server, :ok ->
      case Server.detach(server) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp stop_servers(servers) do
    Enum.each(servers, fn server ->
      try do
        Server.stop(server)
      catch
        :exit, _ -> :ok
      end
    end)
  end

  defp extract_gen_opts(opts) do
    case Keyword.pop(opts, :name) do
      {nil, rest} -> {[], rest}
      {name, rest} -> {[name: name], rest}
    end
  end
end
