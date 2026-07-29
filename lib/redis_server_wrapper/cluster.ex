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
    * `:username` - optional ACL username paired with `:password`
    * `:tls` - use TLS-only cluster node connections (default: false)
    * `:tls_cert_file`, `:tls_key_file` - server certificate and private key
    * `:tls_ca_cert_file` or `:tls_ca_cert_dir` - trusted CA for Redis and redis-cli
    * `:tls_client_cert_file`, `:tls_client_key_file` - optional redis-cli client identity
    * `:tls_server_name` - optional redis-cli SNI name
    * `:tls_insecure` - explicitly disable redis-cli certificate verification
    * `:redis_server_bin` - redis-server binary path
    * `:redis_cli_bin` - redis-cli binary path
    * `:distribution` - `:core` (default), `:full`, or `:legacy_stack`
    * `:timeout` - startup timeout per node in ms (default: 10_000)
    * `:convergence_timeout` - bounded wait for every node to agree on the
      cluster topology (default: the value of `:timeout`)
    * `:cluster_node_timeout` - cluster node timeout in ms (default: 5000)
    * `:loadmodule` - modules loaded into every cluster node; accepts paths or
      `{path, [args]}` tuples (default: `[]`)
    * `:extra` - extra redis config directives as `[{key, value}]`
    * `:managed` - process lifecycle backend forwarded to every node. See
      `RedisServerWrapper.Server` for `true`, `:forcola`, and `false`.
      Only `managed: false` supports `detach/1`.
  """

  use GenServer

  alias RedisServerWrapper.{Cli, Config, Connection, Server}

  require Logger

  defstruct [
    :masters,
    :replicas_per_master,
    :base_port,
    :bind,
    :control_host,
    :connection,
    :username,
    :password,
    :redis_cli_bin,
    node_pids: [],
    node_ports: %{},
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

  @doc "Checks if every tracked node process is alive and responds to PING."
  @spec all_alive?(GenServer.server()) :: boolean()
  def all_alive?(server), do: GenServer.call(server, :all_alive?)

  @doc """
  Checks every node's `CLUSTER INFO`, including state, slots, failures,
  expected node count, and expected master count.
  """
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
    username = Keyword.get(opts, :username)
    distribution = Keyword.get(opts, :distribution, :core)
    redis_cli_bin = Keyword.get(opts, :redis_cli_bin, "redis-cli")
    timeout = Keyword.get(opts, :timeout, 10_000)
    convergence_timeout = Keyword.get(opts, :convergence_timeout, timeout)
    cluster_node_timeout = Keyword.get(opts, :cluster_node_timeout, 5000)
    loadmodule = Keyword.get(opts, :loadmodule, [])
    extra = Keyword.get(opts, :extra, [])
    managed = Keyword.get(opts, :managed, true)

    with :ok <- valid_port(:base_port, base_port),
         :ok <- reject_unix_transport(opts),
         {:ok, connection} <- build_connection(opts, control_host, base_port),
         :ok <- Server.validate_distribution(distribution),
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
           connection: connection,
           username: username,
           password: password,
           tls: connection.transport == :tls,
           tls_cert_file: Keyword.get(opts, :tls_cert_file),
           tls_key_file: Keyword.get(opts, :tls_key_file),
           tls_ca_cert_file: Keyword.get(opts, :tls_ca_cert_file),
           tls_ca_cert_dir: Keyword.get(opts, :tls_ca_cert_dir),
           tls_auth_clients: Keyword.get(opts, :tls_auth_clients),
           tls_client_cert_file: Keyword.get(opts, :tls_client_cert_file),
           tls_client_key_file: Keyword.get(opts, :tls_client_key_file),
           tls_server_name: Keyword.get(opts, :tls_server_name),
           tls_insecure: Keyword.get(opts, :tls_insecure, false),
           distribution: distribution,
           redis_server_bin: redis_server_bin,
           redis_cli_bin: redis_cli_bin,
           timeout: timeout,
           convergence_timeout: convergence_timeout,
           cluster_node_timeout: cluster_node_timeout,
           loadmodule: loadmodule,
           extra: extra,
           managed: managed
         },
         :ok <- validate_server_connection_config(settings),
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
        connection = Server.info(pid).connection
        format_addr(connection.host, connection.port)
      end)

    {:reply, addrs, state}
  end

  def handle_call(:nodes, _from, state) do
    {:reply, state.node_pids, state}
  end

  def handle_call(:all_alive?, _from, state) do
    expected_nodes = state.masters * (1 + state.replicas_per_master)

    all =
      length(state.node_pids) == expected_nodes and
        Enum.all?(state.node_pids, &safe_server_ping/1)

    {:reply, all, state}
  end

  def handle_call(:healthy?, _from, state) do
    expected_nodes = state.masters * (1 + state.replicas_per_master)

    result =
      match?(
        {:ok, _snapshot},
        cluster_health(state.node_pids, state.masters, expected_nodes)
      )

    {:reply, result, state}
  end

  def handle_call(:info, _from, state) do
    info = %{
      masters: state.masters,
      replicas_per_master: state.replicas_per_master,
      base_port: state.base_port,
      bind: state.bind,
      control_host: state.control_host,
      connection: state.connection,
      total_nodes: length(state.node_pids),
      node_addrs:
        Enum.map(state.node_pids, fn pid ->
          connection = Server.info(pid).connection
          format_addr(connection.host, connection.port)
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
    case Enum.find_index(state.node_pids, &(&1 == pid)) do
      nil ->
        {:noreply, state}

      index ->
        port = Map.get(state.node_ports, pid, state.base_port + index)

        Logger.warning(
          "Cluster node on port #{port} exited; topology is degraded: #{inspect(reason)}"
        )

        {:noreply,
         %{
           state
           | node_pids: List.delete(state.node_pids, pid),
             node_ports: Map.delete(state.node_ports, pid)
         }}
    end
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
        :connection,
        :username,
        :password,
        :tls,
        :tls_cert_file,
        :tls_key_file,
        :tls_ca_cert_file,
        :tls_ca_cert_dir,
        :tls_auth_clients,
        :tls_client_cert_file,
        :tls_client_key_file,
        :tls_server_name,
        :tls_insecure,
        :distribution,
        :redis_server_bin,
        :redis_cli_bin,
        :timeout,
        :convergence_timeout,
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
    seed_cli = node_pids |> List.first() |> Server.cli()

    node_addr_list = Enum.map(ports, &format_addr(settings.control_host, &1))

    case Cli.cluster_create(seed_cli, node_addr_list, settings.replicas_per_master) do
      {:ok, _output} ->
        case await_cluster_convergence(
               node_pids,
               settings.masters,
               settings.convergence_timeout
             ) do
          {:ok, _snapshot} ->
            {:ok,
             %__MODULE__{
               masters: settings.masters,
               replicas_per_master: settings.replicas_per_master,
               base_port: settings.base_port,
               bind: settings.bind,
               control_host: settings.control_host,
               connection: settings.connection,
               username: settings.username,
               password: settings.password,
               redis_cli_bin: settings.redis_cli_bin,
               node_pids: node_pids,
               node_ports: Map.new(Enum.zip(node_pids, ports))
             }}

          {:error, last_health} ->
            stop_servers(node_pids)

            {:stop, {:cluster_convergence_timeout, settings.convergence_timeout, last_health}}
        end

      {:error, reason} ->
        # Rollback: stop all nodes
        stop_servers(node_pids)
        {:stop, {:cluster_create_failed, reason}}
    end
  end

  defp start_nodes(ports, node_opts) do
    results =
      Enum.reduce_while(ports, {:ok, []}, fn port, {:ok, acc} ->
        opts =
          connection_server_opts(node_opts, port) ++
            [
              bind: node_opts.bind,
              control_host: node_opts.control_host,
              distribution: node_opts.distribution,
              redis_server_bin: node_opts.redis_server_bin,
              redis_cli_bin: node_opts.redis_cli_bin,
              timeout: node_opts.timeout,
              managed: node_opts.managed,
              cluster_enabled: true,
              cluster_config_file: "nodes-#{port}.conf",
              cluster_node_timeout: node_opts.cluster_node_timeout,
              cluster_announce_port: port,
              loadmodule: node_opts.loadmodule,
              save: :disabled
            ] ++ extra_to_opts(node_opts.extra)

        case Server.start_link(opts) do
          {:ok, pid} ->
            {:cont, {:ok, acc ++ [pid]}}

          {:error, reason} ->
            # Rollback already-started nodes
            stop_servers(acc)
            {:halt, {:error, {:node_start_failed, port, reason}}}
        end
      end)

    results
  end

  defp seed_cli(%{node_pids: [server | _rest]}) do
    Server.cli(server)
  end

  defp seed_cli(state) do
    Cli.new(
      bin: state.redis_cli_bin,
      connection: state.connection
    )
  end

  defp connection_server_opts(%{tls: false} = opts, port) do
    [
      port: port,
      username: opts.username,
      password: opts.password
    ]
  end

  defp connection_server_opts(%{tls: true} = opts, port) do
    [
      port: 0,
      tls_port: port,
      username: opts.username,
      password: opts.password,
      tls_cert_file: opts.tls_cert_file,
      tls_key_file: opts.tls_key_file,
      tls_ca_cert_file: opts.tls_ca_cert_file,
      tls_ca_cert_dir: opts.tls_ca_cert_dir,
      tls_auth_clients: opts.tls_auth_clients,
      tls_client_cert_file: opts.tls_client_cert_file,
      tls_client_key_file: opts.tls_client_key_file,
      tls_server_name: opts.tls_server_name,
      tls_insecure: opts.tls_insecure,
      tls_cluster: true
    ]
  end

  defp build_connection(opts, host, port) do
    transport = if Keyword.get(opts, :tls, false), do: :tls, else: :tcp

    connection_opts = [
      transport: transport,
      host: host,
      port: port,
      username: Keyword.get(opts, :username),
      password: Keyword.get(opts, :password),
      tls_ca_cert_file: Keyword.get(opts, :tls_ca_cert_file),
      tls_ca_cert_dir: Keyword.get(opts, :tls_ca_cert_dir),
      tls_client_cert_file: Keyword.get(opts, :tls_client_cert_file),
      tls_client_key_file: Keyword.get(opts, :tls_client_key_file),
      tls_server_name: Keyword.get(opts, :tls_server_name),
      tls_insecure: Keyword.get(opts, :tls_insecure, false)
    ]

    {:ok, Connection.new(connection_opts)}
  rescue
    error in ArgumentError -> {:error, {:invalid_connection, Exception.message(error)}}
  end

  defp reject_unix_transport(opts) do
    if Keyword.get(opts, :unixsocket) || Keyword.get(opts, :transport) == :unix do
      {:error, {:unsupported_transport, :cluster, :unix}}
    else
      :ok
    end
  end

  defp validate_server_connection_config(settings) do
    _config =
      Config.new(
        connection_server_opts(settings, settings.base_port) ++
          [bind: settings.bind, control_host: settings.control_host]
      )

    :ok
  rescue
    error in ArgumentError ->
      {:error, {:invalid_connection_config, Exception.message(error)}}
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

  defp await_cluster_convergence(node_pids, masters, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_cluster_convergence(node_pids, masters, deadline, nil)
  end

  defp do_await_cluster_convergence(node_pids, masters, deadline, _last_health) do
    case cluster_health(node_pids, masters, length(node_pids)) do
      {:ok, _snapshot} = healthy ->
        healthy

      {:error, _reason} = unhealthy ->
        if System.monotonic_time(:millisecond) >= deadline do
          unhealthy
        else
          Process.sleep(100)
          do_await_cluster_convergence(node_pids, masters, deadline, unhealthy)
        end
    end
  end

  defp cluster_health(node_pids, masters, expected_nodes)
       when length(node_pids) == expected_nodes do
    node_pids
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {pid, index}, {:ok, snapshots} ->
      with {:ok, cli} <- safe_server_cli(pid),
           true <- Cli.ping(cli),
           {:ok, info} <- Cli.cluster_info(cli),
           :ok <- valid_cluster_info(info, expected_nodes, masters) do
        {:cont, {:ok, [info | snapshots]}}
      else
        false ->
          {:halt, {:error, {:node_unreachable, index}}}

        {:error, reason} ->
          {:halt, {:error, {:node_health_failed, index, reason}}}
      end
    end)
    |> case do
      {:ok, snapshots} -> {:ok, Enum.reverse(snapshots)}
      {:error, _reason} = error -> error
    end
  end

  defp cluster_health(node_pids, _masters, expected_nodes),
    do: {:error, {:unexpected_node_count, expected_nodes, length(node_pids)}}

  defp valid_cluster_info(info, expected_nodes, masters) do
    expected = %{
      "cluster_state" => "ok",
      "cluster_slots_assigned" => 16_384,
      "cluster_slots_ok" => 16_384,
      "cluster_slots_pfail" => 0,
      "cluster_slots_fail" => 0,
      "cluster_known_nodes" => expected_nodes,
      "cluster_size" => masters
    }

    invalid =
      Enum.reduce(expected, %{}, fn
        {"cluster_state" = key, value}, acc ->
          if Map.get(info, key) == value,
            do: acc,
            else: Map.put(acc, key, Map.get(info, key))

        {key, value}, acc ->
          case parse_integer(Map.get(info, key)) do
            {:ok, ^value} -> acc
            _other -> Map.put(acc, key, Map.get(info, key))
          end
      end)

    if map_size(invalid) == 0 do
      :ok
    else
      {:error, {:invalid_cluster_info, invalid}}
    end
  end

  defp safe_server_cli(server) do
    {:ok, Server.cli(server)}
  catch
    :exit, reason -> {:error, {:server_exit, reason}}
  end

  defp safe_server_ping(server) do
    Server.ping(server)
  catch
    :exit, _reason -> false
  end

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _other -> :error
    end
  end

  defp parse_integer(_value), do: :error

  defp validate_options(settings) do
    with :ok <- positive_integer(:masters, settings.masters),
         :ok <-
           non_negative_integer(:replicas_per_master, settings.replicas_per_master),
         :ok <- valid_port(:base_port, settings.base_port),
         :ok <- positive_integer(:timeout, settings.timeout),
         :ok <- positive_integer(:convergence_timeout, settings.convergence_timeout),
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
