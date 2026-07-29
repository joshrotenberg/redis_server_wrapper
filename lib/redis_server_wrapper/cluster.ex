defmodule RedisServerWrapper.Cluster do
  @moduledoc """
  GenServer managing a Redis Cluster (multiple redis-server nodes in cluster mode).

  Starts N master nodes (with optional replicas), then uses `redis-cli --cluster create`
  to form the cluster.

  ## Usage

      {:ok, pid} = RedisServerWrapper.Cluster.start_link(
        masters: 3,
        replicas_per_master: 1,
        base_port: 7100
      )

      RedisServerWrapper.Cluster.healthy?(pid)
      RedisServerWrapper.Cluster.node_addrs(pid)
      RedisServerWrapper.Cluster.stop(pid)

  ## Options

    * `:masters` - number of master nodes (default: 3)
    * `:replicas_per_master` - replicas per master (default: 0)
    * `:base_port` - starting port (default: 7100). Note: 7000 is avoided
      because macOS AirPlay Receiver binds it by default, which causes
      confusing cluster-start failures on Mac.
    * `:bind` - bind address (default: "127.0.0.1")
    * `:control_host` - address used by redis-cli and cluster announcements
      (default: first bind address)
    * `:password` - Redis password (default: nil)
    * `:redis_server_bin` - redis-server binary path
    * `:redis_cli_bin` - redis-cli binary path
    * `:distribution` - `:core` (default), `:full`, or `:legacy_stack`
    * `:timeout` - startup timeout per node in ms (default: 10_000)
    * `:cluster_node_timeout` - cluster node timeout in ms (default: 5000)
    * `:loadmodule` - modules loaded into every cluster node; accepts paths or
      `{path, [args]}` tuples (default: `[]`)
    * `:extra` - extra redis config directives as `[{key, value}]`
    * `:managed` - process lifecycle backend forwarded to every node. See
      `RedisServerWrapper.Server` for `true`, `:forcola`, and `false`.
      Only `managed: false` supports `detach/1`.
  """

  use GenServer

  alias RedisServerWrapper.{Cli, Config, Server}

  require Logger

  defstruct [
    :masters,
    :replicas_per_master,
    :base_port,
    :bind,
    :control_host,
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

  @doc "Detaches daemonized cluster nodes so their OS processes survive this GenServer."
  @spec detach(GenServer.server()) :: :ok | {:error, :managed_server}
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
    base_port = Keyword.get(opts, :base_port, 7100)
    bind = Keyword.get(opts, :bind, "127.0.0.1")

    control_host =
      Config.new(bind: bind, control_host: Keyword.get(opts, :control_host))
      |> Config.control_host()

    password = Keyword.get(opts, :password)
    distribution = Keyword.get(opts, :distribution, :core)
    redis_cli_bin = Keyword.get(opts, :redis_cli_bin, "redis-cli")
    timeout = Keyword.get(opts, :timeout, 10_000)
    cluster_node_timeout = Keyword.get(opts, :cluster_node_timeout, 5000)
    loadmodule = Keyword.get(opts, :loadmodule, [])
    extra = Keyword.get(opts, :extra, [])
    managed = Keyword.get(opts, :managed, true)

    with :ok <- Server.validate_distribution(distribution),
         redis_server_bin =
           Keyword.get_lazy(opts, :redis_server_bin, fn ->
             Server.default_server_bin(distribution)
           end),
         settings = %{
           masters: masters,
           replicas_per_master: replicas,
           base_port: base_port,
           bind: bind,
           control_host: control_host,
           password: password,
           distribution: distribution,
           redis_server_bin: redis_server_bin,
           redis_cli_bin: redis_cli_bin,
           timeout: timeout,
           cluster_node_timeout: cluster_node_timeout,
           loadmodule: loadmodule,
           extra: extra,
           managed: managed
         },
         :ok <- validate_options(settings) do
      start_validated_cluster(settings)
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:addr, _from, state) do
    {:reply, format_addr(state.control_host, state.base_port), state}
  end

  def handle_call(:node_addrs, _from, state) do
    addrs =
      Enum.map(state.node_pids, fn pid ->
        info = Server.info(pid)
        format_addr(info.host, info.port)
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
      control_host: state.control_host,
      total_nodes: length(state.node_pids),
      node_addrs:
        Enum.map(state.node_pids, fn pid ->
          node_info = Server.info(pid)
          format_addr(node_info.host, node_info.port)
        end)
    }

    {:reply, info, state}
  end

  def handle_call({:run, args}, _from, state) do
    {:reply, Cli.run(seed_cli(state), args), state}
  end

  def handle_call(:detach, _from, state) do
    case detach_servers(state.node_pids) do
      :ok -> {:reply, :ok, %{state | detached: true}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    if pid in state.node_pids and reason != :normal do
      Logger.warning("Cluster node #{inspect(pid)} exited: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{detached: true} = state) do
    Logger.debug("RedisServerWrapper.Cluster terminating (OS processes detached)")
    stop_servers(state.node_pids)
    :ok
  end

  def terminate(_reason, state) do
    Logger.debug(
      "RedisServerWrapper.Cluster terminating, stopping #{length(state.node_pids)} nodes"
    )

    stop_servers(state.node_pids)

    :ok
  end

  # -------------------------------------------------------------------
  # Internal
  # -------------------------------------------------------------------

  defp start_validated_cluster(settings) do
    total_nodes = settings.masters * (1 + settings.replicas_per_master)
    ports = ports_from(settings.base_port, total_nodes)

    node_opts =
      Map.take(settings, [
        :bind,
        :control_host,
        :password,
        :distribution,
        :redis_server_bin,
        :redis_cli_bin,
        :timeout,
        :cluster_node_timeout,
        :loadmodule,
        :extra,
        :managed
      ])

    # Server startup fails closed if a requested port is already occupied;
    # Cluster never shuts down or signals an existing listener to claim it.
    case start_nodes(ports, node_opts) do
      {:ok, node_pids} -> form_cluster(node_pids, ports, settings)
      {:error, reason} -> {:stop, reason}
    end
  end

  defp form_cluster(node_pids, ports, settings) do
    seed_cli =
      Cli.new(
        bin: settings.redis_cli_bin,
        host: settings.control_host,
        port: settings.base_port,
        password: settings.password
      )

    node_addr_list = Enum.map(ports, &format_addr(settings.control_host, &1))

    case Cli.cluster_create(seed_cli, node_addr_list, settings.replicas_per_master) do
      {:ok, _output} ->
        # Wait for cluster convergence
        Process.sleep(2000)

        state = %__MODULE__{
          masters: settings.masters,
          replicas_per_master: settings.replicas_per_master,
          base_port: settings.base_port,
          bind: settings.bind,
          control_host: settings.control_host,
          password: settings.password,
          redis_cli_bin: settings.redis_cli_bin,
          node_pids: node_pids
        }

        {:ok, state}

      {:error, reason} ->
        # Rollback: stop all nodes
        Enum.each(node_pids, &Server.stop/1)
        {:stop, {:cluster_create_failed, reason}}
    end
  end

  defp start_nodes(ports, node_opts) do
    results =
      Enum.reduce_while(ports, {:ok, []}, fn port, {:ok, acc} ->
        opts =
          [
            port: port,
            bind: node_opts.bind,
            control_host: node_opts.control_host,
            password: node_opts.password,
            distribution: node_opts.distribution,
            redis_server_bin: node_opts.redis_server_bin,
            redis_cli_bin: node_opts.redis_cli_bin,
            timeout: node_opts.timeout,
            managed: node_opts.managed,
            cluster_enabled: true,
            cluster_config_file: "nodes-#{port}.conf",
            cluster_node_timeout: node_opts.cluster_node_timeout,
            loadmodule: node_opts.loadmodule,
            save: :disabled
          ] ++ extra_to_opts(node_opts.extra)

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

  defp seed_cli(state) do
    Cli.new(
      bin: state.redis_cli_bin,
      host: state.control_host,
      port: state.base_port,
      password: state.password
    )
  end

  defp extra_to_opts([]), do: []
  defp extra_to_opts(extra), do: [extra: extra]

  defp format_addr(host, port) do
    if String.contains?(host, ":") do
      "[#{host}]:#{port}"
    else
      "#{host}:#{port}"
    end
  end

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

  defp validate_options(settings) do
    with :ok <- positive_integer(:masters, settings.masters),
         :ok <-
           non_negative_integer(:replicas_per_master, settings.replicas_per_master),
         :ok <- valid_port(:base_port, settings.base_port),
         :ok <- positive_integer(:timeout, settings.timeout),
         :ok <- positive_integer(:cluster_node_timeout, settings.cluster_node_timeout) do
      total_nodes = settings.masters * (1 + settings.replicas_per_master)
      last_data_port = settings.base_port + total_nodes - 1
      first_bus_port = settings.base_port + 10_000
      last_bus_port = last_data_port + 10_000

      cond do
        last_data_port > 65_535 ->
          {:error, {:invalid_port_range, :cluster_nodes, settings.base_port, last_data_port}}

        last_data_port >= first_bus_port ->
          {:error,
           {:overlapping_port_ranges, :cluster_nodes, settings.base_port, last_data_port,
            :cluster_bus, first_bus_port, last_bus_port}}

        last_bus_port > 65_535 ->
          {:error, {:invalid_port_range, :cluster_bus, first_bus_port, last_bus_port}}

        true ->
          :ok
      end
    end
  end

  defp positive_integer(_key, value) when is_integer(value) and value > 0, do: :ok
  defp positive_integer(key, value), do: {:error, {:invalid_option, key, value}}

  defp non_negative_integer(_key, value) when is_integer(value) and value >= 0, do: :ok
  defp non_negative_integer(key, value), do: {:error, {:invalid_option, key, value}}

  defp valid_port(_key, value) when is_integer(value) and value in 1..65_535, do: :ok
  defp valid_port(key, value), do: {:error, {:invalid_option, key, value}}

  defp ports_from(_base_port, 0), do: []
  defp ports_from(base_port, count), do: Enum.map(0..(count - 1), &(base_port + &1))

  defp extract_gen_opts(opts) do
    case Keyword.pop(opts, :name) do
      {nil, rest} -> {[], rest}
      {name, rest} -> {[name: name], rest}
    end
  end
end
