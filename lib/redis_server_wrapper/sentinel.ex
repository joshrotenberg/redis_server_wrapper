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
    * `:control_host` - address used by redis-cli, replication, and Sentinel
      monitoring (default: first bind address)
    * `:password` - Redis password
    * `:username` - optional ACL username paired with `:password`
    * `:tls` - use TLS-only data-node and Sentinel connections (default: false)
    * `:tls_cert_file`, `:tls_key_file` - server certificate and private key
    * `:tls_ca_cert_file` or `:tls_ca_cert_dir` - trusted CA for Redis and redis-cli
    * `:tls_client_cert_file`, `:tls_client_key_file` - optional redis-cli client identity
    * `:tls_server_name` - optional redis-cli SNI name
    * `:tls_insecure` - explicitly disable redis-cli certificate verification
    * `:redis_server_bin` - redis-server binary path
    * `:redis_cli_bin` - redis-cli binary path
    * `:distribution` - `:core` (default), `:full`, or `:legacy_stack`
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

  alias RedisServerWrapper.{Cli, Config, Connection, OSProcess, SecureFile, Server}

  require Logger

  defstruct [
    :master_name,
    :master_port,
    :bind,
    :control_host,
    :master_connection,
    :sentinel_connection,
    :username,
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

    control_host =
      Config.new(bind: bind, control_host: Keyword.get(opts, :control_host))
      |> Config.control_host()

    password = Keyword.get(opts, :password)
    username = Keyword.get(opts, :username)
    distribution = Keyword.get(opts, :distribution, :core)
    redis_cli_bin = Keyword.get(opts, :redis_cli_bin, "redis-cli")
    timeout = Keyword.get(opts, :timeout, 10_000)
    loadmodule = Keyword.get(opts, :loadmodule, [])
    managed = Keyword.get(opts, :managed, true)

    with :ok <- valid_port(:master_port, master_port),
         :ok <- reject_unix_transport(opts),
         {:ok, master_connection} <- build_connection(opts, control_host, master_port),
         :ok <- Server.validate_distribution(distribution),
         redis_server_bin =
           Keyword.get_lazy(opts, :redis_server_bin, fn ->
             Server.default_server_bin(distribution)
           end),
         node_opts = %{
           bind: bind,
           control_host: control_host,
           username: username,
           password: password,
           tls: master_connection.transport == :tls,
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
           loadmodule: loadmodule,
           managed: managed
         },
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
         },
         :ok <- validate_server_connection_config(node_opts, master_port),
         :ok <- validate_options(validation_settings),
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
             control_host: control_host,
             username: username,
             password: password,
             tls: master_connection.transport == :tls,
             tls_cert_file: Keyword.get(opts, :tls_cert_file),
             tls_key_file: Keyword.get(opts, :tls_key_file),
             tls_ca_cert_file: Keyword.get(opts, :tls_ca_cert_file),
             tls_ca_cert_dir: Keyword.get(opts, :tls_ca_cert_dir),
             tls_auth_clients: Keyword.get(opts, :tls_auth_clients),
             tls_client_cert_file: Keyword.get(opts, :tls_client_cert_file),
             tls_client_key_file: Keyword.get(opts, :tls_client_key_file),
             tls_server_name: Keyword.get(opts, :tls_server_name),
             tls_insecure: Keyword.get(opts, :tls_insecure, false),
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
      sentinel_connection = Connection.with_port(master_connection, sentinel_base_port)

      state = %__MODULE__{
        master_name: master_name,
        master_port: master_port,
        bind: bind,
        control_host: control_host,
        master_connection: master_connection,
        sentinel_connection: sentinel_connection,
        username: username,
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
    {:reply, format_addr(state.control_host, state.master_port), state}
  end

  def handle_call(:sentinel_addrs, _from, state) do
    addrs = Enum.map(state.sentinel_ports, &format_addr(state.control_host, &1))
    {:reply, addrs, state}
  end

  def handle_call(:info, _from, state) do
    info = %{
      master_name: state.master_name,
      bind: state.bind,
      control_host: state.control_host,
      master_connection: state.master_connection,
      sentinel_connection: state.sentinel_connection,
      master_addr: format_addr(state.control_host, state.master_port),
      replicas: state.num_replicas,
      sentinels: state.num_sentinels,
      sentinel_addrs: Enum.map(state.sentinel_ports, &format_addr(state.control_host, &1))
    }

    {:reply, info, state}
  end

  def handle_call(:healthy?, _from, state) do
    result =
      Enum.any?(state.sentinel_ports, fn port ->
        cli =
          Cli.new(
            bin: state.redis_cli_bin,
            connection: Connection.with_port(state.sentinel_connection, port)
          )

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
        cli =
          Cli.new(
            bin: state.redis_cli_bin,
            connection: Connection.with_port(state.sentinel_connection, port)
          )

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
      connection_server_opts(node_opts, port) ++
        [
          bind: node_opts.bind,
          control_host: node_opts.control_host,
          distribution: node_opts.distribution,
          redis_server_bin: node_opts.redis_server_bin,
          redis_cli_bin: node_opts.redis_cli_bin,
          timeout: node_opts.timeout,
          managed: node_opts.managed,
          loadmodule: node_opts.loadmodule,
          save: :disabled
        ]
    )
  end

  defp start_replicas(0, _base_port, _master_port, _node_opts), do: {:ok, []}

  defp start_replicas(count, base_port, master_port, node_opts) do
    results =
      Enum.reduce_while(0..(count - 1), {:ok, []}, fn i, {:ok, acc} ->
        port = base_port + i

        opts =
          connection_server_opts(node_opts, port) ++
            [
              bind: node_opts.bind,
              control_host: node_opts.control_host,
              distribution: node_opts.distribution,
              masteruser: node_opts.username,
              masterauth: node_opts.password,
              replicaof: {node_opts.control_host, master_port},
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

    SecureFile.make_private_directory!(sentinel_dir)

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
    SecureFile.make_private_directory!(node_dir)

    conf_content = generate_sentinel_conf(opts, node_dir, port)
    conf_path = Path.join(node_dir, "sentinel.conf")
    SecureFile.write_private!(conf_path, conf_content)

    start_sentinel_process(
      opts.redis_server_bin,
      conf_path,
      node_dir,
      opts.redis_cli_bin,
      sentinel_connection(opts, port),
      opts.timeout
    )
  end

  defp generate_sentinel_conf(opts, dir, port) do
    %{
      bind: bind,
      control_host: control_host,
      master_name: master_name,
      master_port: master_port,
      username: username,
      password: password,
      tls: tls,
      quorum: quorum,
      down_after_ms: down_after_ms,
      failover_timeout_ms: failover_timeout_ms
    } = opts

    listen_addresses = Config.new(bind: bind) |> Config.listen_addresses()

    lines =
      [
        Config.directive("port", [if(tls, do: 0, else: port)]),
        Config.directive("bind", listen_addresses),
        Config.directive("daemonize", ["yes"]),
        Config.directive("pidfile", [Path.join(dir, "sentinel.pid")]),
        Config.directive("logfile", [Path.join(dir, "sentinel.log")]),
        Config.directive("dir", [dir]),
        Config.directive("sentinel", ["monitor", master_name, control_host, master_port, quorum]),
        Config.directive("sentinel", ["down-after-milliseconds", master_name, down_after_ms]),
        Config.directive("sentinel", ["failover-timeout", master_name, failover_timeout_ms]),
        Config.directive("sentinel", ["parallel-syncs", master_name, 1])
      ] ++ sentinel_tls_lines(opts, port) ++ sentinel_auth_lines(username, password, master_name)

    Enum.join(lines, "\n") <> "\n"
  end

  defp sentinel_tls_lines(%{tls: false}, _port), do: []

  defp sentinel_tls_lines(opts, port) do
    [
      Config.directive("tls-port", [port]),
      Config.directive("tls-cert-file", [opts.tls_cert_file]),
      Config.directive("tls-key-file", [opts.tls_key_file])
    ] ++
      optional_directive("tls-ca-cert-file", opts.tls_ca_cert_file) ++
      optional_directive("tls-ca-cert-dir", opts.tls_ca_cert_dir) ++
      optional_directive("tls-auth-clients", opts.tls_auth_clients) ++
      [Config.directive("tls-replication", ["yes"])]
  end

  defp sentinel_auth_lines(nil, nil, _master_name), do: []

  defp sentinel_auth_lines(nil, password, master_name) do
    [
      Config.directive("requirepass", [password]),
      Config.directive("sentinel", ["auth-pass", master_name, password])
    ]
  end

  defp sentinel_auth_lines(username, password, master_name) do
    [
      Config.directive("user", ["default", "off"]),
      Config.directive("user", [username, "on", ">#{password}", "~*", "&*", "+@all"]),
      Config.directive("sentinel", ["auth-user", master_name, username]),
      Config.directive("sentinel", ["auth-pass", master_name, password])
    ]
  end

  defp optional_directive(_key, nil), do: []
  defp optional_directive(key, value), do: [Config.directive(key, [value])]

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
         connection,
         timeout
       ) do
    # Sentinel rewrites its configuration as topology state changes. Launch it
    # with a private umask so every replacement keeps credential-bearing config
    # private, not only the initial file written above.
    command_args = [
      "-c",
      ~S(umask 077; exec "$@"),
      "redis-server-wrapper",
      redis_server_bin,
      conf_path,
      "--sentinel"
    ]

    case System.cmd("/bin/sh", command_args, stderr_to_stdout: true) do
      {_output, 0} ->
        # Wait for sentinel to be ready
        cli = Cli.new(bin: redis_cli_bin, connection: connection)

        case Cli.wait_for_ready(cli, timeout) do
          :ok ->
            pid_path = Path.join(node_dir, "sentinel.pid")
            pid = read_pidfile(pid_path)
            {:ok, pid}

          {:error, :timeout} ->
            {:error, {:sentinel_start_timeout, connection.port}}

          {:error, {:unexpected_reply, reply}} ->
            {:error, {:sentinel_port_in_use, connection.port, reply}}
        end

      {output, code} ->
        {:error, {:sentinel_start_failed, connection.port, code, output}}
    end
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
      tls_replication: true
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

  defp sentinel_connection(opts, port) do
    {:ok, connection} = build_connection(Map.to_list(opts), opts.control_host, port)
    connection
  end

  defp reject_unix_transport(opts) do
    if Keyword.get(opts, :unixsocket) || Keyword.get(opts, :transport) == :unix do
      {:error, {:unsupported_transport, :sentinel, :unix}}
    else
      :ok
    end
  end

  defp validate_server_connection_config(node_opts, port) do
    _config =
      Config.new(
        connection_server_opts(node_opts, port) ++
          [bind: node_opts.bind, control_host: node_opts.control_host]
      )

    :ok
  rescue
    error in ArgumentError ->
      {:error, {:invalid_connection_config, Exception.message(error)}}
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

  defp extract_gen_opts(opts) do
    case Keyword.pop(opts, :name) do
      {nil, rest} -> {[], rest}
      {name, rest} -> {[name: name], rest}
    end
  end
end
