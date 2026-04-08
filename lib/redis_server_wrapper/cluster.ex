defmodule RedisServerWrapper.Cluster do
  @moduledoc """
  GenServer managing a Redis Cluster (multiple redis-server nodes in cluster mode).

  Starts N master nodes (with optional replicas), then uses `redis-cli --cluster create`
  to form the cluster.

  ## Usage

      {:ok, pid} = RedisServerWrapper.Cluster.start_link(
        masters: 3,
        replicas_per_master: 1,
        base_port: 7000
      )

      RedisServerWrapper.Cluster.healthy?(pid)
      RedisServerWrapper.Cluster.node_addrs(pid)
      RedisServerWrapper.Cluster.stop(pid)

  ## Options

    * `:masters` - number of master nodes (default: 3)
    * `:replicas_per_master` - replicas per master (default: 0)
    * `:base_port` - starting port (default: 7000)
    * `:bind` - bind address (default: "127.0.0.1")
    * `:password` - Redis password (default: nil)
    * `:redis_server_bin` - redis-server binary path
    * `:redis_cli_bin` - redis-cli binary path
    * `:timeout` - startup timeout per node in ms (default: 10_000)
    * `:cluster_node_timeout` - cluster node timeout in ms (default: 5000)
    * `:extra` - extra redis config directives as `[{key, value}]`
  """

  use GenServer

  alias RedisServerWrapper.{Cli, Server}

  require Logger

  defstruct [
    :masters,
    :replicas_per_master,
    :base_port,
    :bind,
    :password,
    :redis_cli_bin,
    node_pids: [],
    detached: false
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, cluster_opts} = extract_gen_opts(opts)
    GenServer.start_link(__MODULE__, cluster_opts, gen_opts)
  end

  @spec start(keyword()) :: GenServer.on_start()
  def start(opts \\ []) do
    {gen_opts, cluster_opts} = extract_gen_opts(opts)
    GenServer.start(__MODULE__, cluster_opts, gen_opts)
  end

  @doc "Returns the seed node address (first node)."
  @spec addr(GenServer.server()) :: String.t()
  def addr(server), do: GenServer.call(server, :addr)

  @doc "Returns all node addresses."
  @spec node_addrs(GenServer.server()) :: [String.t()]
  def node_addrs(server), do: GenServer.call(server, :node_addrs)

  @doc "Returns all node PIDs (GenServer pids, not OS pids)."
  @spec nodes(GenServer.server()) :: [pid()]
  def nodes(server), do: GenServer.call(server, :nodes)

  @doc "Checks if all nodes respond to PING."
  @spec all_alive?(GenServer.server()) :: boolean()
  def all_alive?(server), do: GenServer.call(server, :all_alive?)

  @doc "Checks cluster health via CLUSTER INFO (state=ok, all slots assigned)."
  @spec healthy?(GenServer.server()) :: boolean()
  def healthy?(server), do: GenServer.call(server, :healthy?)

  @doc "Returns cluster info map."
  @spec info(GenServer.server()) :: map()
  def info(server), do: GenServer.call(server, :info)

  @doc "Runs a redis-cli command against the seed node."
  @spec run(GenServer.server(), [String.t()]) :: {:ok, String.t()} | {:error, String.t()}
  def run(server, args), do: GenServer.call(server, {:run, args})

  @doc "Detach — cluster processes will not be stopped on terminate."
  @spec detach(GenServer.server()) :: :ok
  def detach(server), do: GenServer.call(server, :detach)

  @doc "Stops the cluster."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server, :normal)

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    masters = Keyword.get(opts, :masters, 3)
    replicas = Keyword.get(opts, :replicas_per_master, 0)
    base_port = Keyword.get(opts, :base_port, 7000)
    bind = Keyword.get(opts, :bind, "127.0.0.1")
    password = Keyword.get(opts, :password)
    redis_server_bin = Keyword.get_lazy(opts, :redis_server_bin, &RedisServerWrapper.Server.default_server_bin/0)
    redis_cli_bin = Keyword.get(opts, :redis_cli_bin, "redis-cli")
    timeout = Keyword.get(opts, :timeout, 10_000)
    cluster_node_timeout = Keyword.get(opts, :cluster_node_timeout, 5000)
    extra = Keyword.get(opts, :extra, [])

    total_nodes = masters * (1 + replicas)
    ports = Enum.map(0..(total_nodes - 1), &(base_port + &1))

    # Pre-cleanup: try to shut down anything on these ports
    cleanup_ports(ports, bind, redis_cli_bin, password)
    Process.sleep(500)

    # Start each node as a Server GenServer
    case start_nodes(
           ports,
           bind,
           password,
           redis_server_bin,
           redis_cli_bin,
           timeout,
           cluster_node_timeout,
           extra
         ) do
      {:ok, node_pids} ->
        # Form the cluster
        seed_cli = Cli.new(bin: redis_cli_bin, host: bind, port: base_port, password: password)
        node_addr_list = Enum.map(ports, &"#{bind}:#{&1}")

        case Cli.cluster_create(seed_cli, node_addr_list, replicas) do
          {:ok, _output} ->
            # Wait for cluster convergence
            Process.sleep(2000)

            state = %__MODULE__{
              masters: masters,
              replicas_per_master: replicas,
              base_port: base_port,
              bind: bind,
              password: password,
              redis_cli_bin: redis_cli_bin,
              node_pids: node_pids
            }

            {:ok, state}

          {:error, reason} ->
            # Rollback: stop all nodes
            Enum.each(node_pids, &Server.stop/1)
            {:stop, {:cluster_create_failed, reason}}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:addr, _from, state) do
    {:reply, "#{state.bind}:#{state.base_port}", state}
  end

  def handle_call(:node_addrs, _from, state) do
    addrs =
      Enum.map(state.node_pids, fn pid ->
        info = Server.info(pid)
        "#{info.host}:#{info.port}"
      end)

    {:reply, addrs, state}
  end

  def handle_call(:nodes, _from, state) do
    {:reply, state.node_pids, state}
  end

  def handle_call(:all_alive?, _from, state) do
    all = Enum.all?(state.node_pids, &Server.ping/1)
    {:reply, all, state}
  end

  def handle_call(:healthy?, _from, state) do
    seed_cli = seed_cli(state)

    result =
      case Cli.cluster_info(seed_cli) do
        {:ok, info} ->
          info["cluster_state"] == "ok" &&
            info["cluster_slots_assigned"] == "16384"

        _ ->
          false
      end

    {:reply, result, state}
  end

  def handle_call(:info, _from, state) do
    info = %{
      masters: state.masters,
      replicas_per_master: state.replicas_per_master,
      base_port: state.base_port,
      bind: state.bind,
      total_nodes: length(state.node_pids),
      node_addrs:
        Enum.map(state.node_pids, fn pid ->
          node_info = Server.info(pid)
          "#{node_info.host}:#{node_info.port}"
        end)
    }

    {:reply, info, state}
  end

  def handle_call({:run, args}, _from, state) do
    {:reply, Cli.run(seed_cli(state), args), state}
  end

  def handle_call(:detach, _from, state) do
    Enum.each(state.node_pids, &Server.detach/1)
    {:reply, :ok, %{state | detached: true}}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    if pid in state.node_pids and reason != :normal do
      Logger.warning("Cluster node #{inspect(pid)} exited: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{detached: true}) do
    Logger.debug("RedisServerWrapper.Cluster terminating (detached)")
    :ok
  end

  def terminate(_reason, state) do
    Logger.debug(
      "RedisServerWrapper.Cluster terminating, stopping #{length(state.node_pids)} nodes"
    )

    Enum.each(state.node_pids, fn pid ->
      try do
        Server.stop(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  # -------------------------------------------------------------------
  # Internal
  # -------------------------------------------------------------------

  defp start_nodes(
         ports,
         bind,
         password,
         redis_server_bin,
         redis_cli_bin,
         timeout,
         cluster_node_timeout,
         extra
       ) do
    results =
      Enum.reduce_while(ports, {:ok, []}, fn port, {:ok, acc} ->
        opts =
          [
            port: port,
            bind: bind,
            password: password,
            redis_server_bin: redis_server_bin,
            redis_cli_bin: redis_cli_bin,
            timeout: timeout,
            cluster_enabled: true,
            cluster_config_file: "nodes-#{port}.conf",
            cluster_node_timeout: cluster_node_timeout,
            save: :disabled
          ] ++ extra_to_opts(extra)

        # Clean any stale cluster config from previous runs
        clean_node_dir(port)

        case Server.start_link(opts) do
          {:ok, pid} ->
            {:cont, {:ok, acc ++ [pid]}}

          {:error, reason} ->
            # Rollback already-started nodes
            Enum.each(acc, &Server.stop/1)
            {:halt, {:error, {:node_start_failed, port, reason}}}
        end
      end)

    results
  end

  defp cleanup_ports(ports, bind, redis_cli_bin, password) do
    Enum.each(ports, fn port ->
      # Try graceful shutdown first
      cli = Cli.new(bin: redis_cli_bin, host: bind, port: port, password: password)
      Cli.shutdown(cli)

      # Force kill anything still on this port
      case System.cmd("lsof", ["-ti", ":#{port}"], stderr_to_stdout: true) do
        {output, 0} ->
          output
          |> String.split("\n", trim: true)
          |> Enum.each(&System.cmd("kill", ["-9", String.trim(&1)], stderr_to_stdout: true))
        _ -> :ok
      end
    end)
  end

  defp seed_cli(state) do
    Cli.new(
      bin: state.redis_cli_bin,
      host: state.bind,
      port: state.base_port,
      password: state.password
    )
  end

  defp extra_to_opts([]), do: []
  defp extra_to_opts(extra), do: [extra: extra]

  # Remove stale cluster config files from a node's data directory.
  # These persist across runs and cause "Node is not empty" errors
  # when trying to create a new cluster.
  defp clean_node_dir(port) do
    # Clean our temp dir
    base = System.tmp_dir!()
    node_dir = Path.join([base, "redis-server-wrapper", "node-#{port}"])
    clean_cluster_files(node_dir, port)

    # Also clean redis-stack-server's default data dir.
    # redis-stack-server writes cluster config to its own dir regardless of
    # what `dir` is set to in our config.
    stack_dir = "/opt/homebrew/var/db/redis-stack"
    clean_cluster_files(stack_dir, port)
  end

  defp clean_cluster_files(dir, port) do
    if File.dir?(dir) do
      for pattern <- ["nodes-#{port}.conf", "nodes-*.conf", "dump.rdb", "appendonly.aof", "appendonlydir"] do
        Path.wildcard(Path.join(dir, pattern))
        |> Enum.each(fn path ->
          if File.dir?(path), do: File.rm_rf!(path), else: File.rm(path)
        end)
      end
    end
  end

  defp extract_gen_opts(opts) do
    case Keyword.pop(opts, :name) do
      {nil, rest} -> {[], rest}
      {name, rest} -> {[name: name], rest}
    end
  end
end
