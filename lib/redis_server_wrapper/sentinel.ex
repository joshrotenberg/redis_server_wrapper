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
    * `:managed` - when `true` (default), master and replicas run as
      Ports tied to the BEAM lifecycle. When `false`, they daemonize
      independently and survive BEAM exits; combine with `detach/1`
      so this GenServer will not tear them down on terminate either.
      Sentinel processes always daemonize regardless of this flag.
  """

  use GenServer

  alias RedisServerWrapper.{Cli, Server}

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

  @doc "Detach — processes will not be stopped on terminate."
  @spec detach(GenServer.server()) :: :ok
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
    replica_base_port = Keyword.get(opts, :replica_base_port, master_port + 1)
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
    managed = Keyword.get(opts, :managed, true)

    all_ports =
      [master_port] ++
        Enum.map(0..(num_replicas - 1), &(replica_base_port + &1)) ++
        Enum.map(0..(num_sentinels - 1), &(sentinel_base_port + &1))

    # Pre-cleanup
    cleanup_ports(all_ports, bind, redis_cli_bin, password)
    Process.sleep(500)

    node_opts = %{
      bind: bind,
      password: password,
      redis_server_bin: redis_server_bin,
      redis_cli_bin: redis_cli_bin,
      timeout: timeout,
      managed: managed
    }

    with {:ok, master_pid} <- start_master(master_port, node_opts),
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

      sentinel_ports = Enum.map(0..(num_sentinels - 1), &(sentinel_base_port + &1))

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
    Server.detach(state.master_pid)
    Enum.each(state.replica_pids, &Server.detach/1)
    {:reply, :ok, %{state | detached: true}}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    if reason != :normal do
      Logger.warning("Sentinel topology process #{inspect(pid)} exited: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{detached: true}) do
    Logger.debug("RedisServerWrapper.Sentinel terminating (detached)")
    :ok
  end

  def terminate(_reason, state) do
    Logger.debug("RedisServerWrapper.Sentinel terminating, stopping topology")

    # Stop sentinels first (they're raw OS processes, not GenServers)
    Enum.each(state.sentinel_os_pids, fn pid ->
      System.cmd("kill", ["-TERM", to_string(pid)], stderr_to_stdout: true)
    end)

    Process.sleep(500)

    # Force kill any remaining sentinel processes
    Enum.each(state.sentinel_os_pids, fn pid ->
      case System.cmd("kill", ["-0", to_string(pid)], stderr_to_stdout: true) do
        {_, 0} -> System.cmd("kill", ["-9", to_string(pid)], stderr_to_stdout: true)
        _ -> :ok
      end
    end)

    # Stop replicas, then master
    Enum.each(state.replica_pids, fn pid ->
      try do
        Server.stop(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    try do
      Server.stop(state.master_pid)
    catch
      :exit, _ -> :ok
    end

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
      System.cmd("kill", ["-TERM", to_string(pid)], stderr_to_stdout: true)
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

  defp cleanup_ports(ports, bind, redis_cli_bin, password) do
    Enum.each(ports, fn port ->
      cli = Cli.new(bin: redis_cli_bin, host: bind, port: port, password: password)
      Cli.shutdown(cli)
    end)
  end

  defp extract_gen_opts(opts) do
    case Keyword.pop(opts, :name) do
      {nil, rest} -> {[], rest}
      {name, rest} -> {[name: name], rest}
    end
  end
end
