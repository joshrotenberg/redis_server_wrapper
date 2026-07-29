defmodule RedisServerWrapper.Manager do
  @moduledoc """
  Persistent instance manager for Redis server processes.

  Tracks instances across IEx sessions via a JSON state file. Instances are
  detached by default so they outlive the Elixir process that started them.

  ## Usage

      Manager.start_basic(port: 6400)
      Manager.start_cluster(masters: 3, base_port: 7100)
      Manager.start_sentinel(master_port: 6390)

      Manager.list()
      Manager.info("redis-basic-1")
      Manager.stop("redis-basic-1")
      Manager.cleanup()

  State is stored at `~/.config/redis-server-wrapper/instances.json`. Mutating
  operations are serialized within a connected BEAM cluster and use atomic
  file replacement. Independent, unconnected BEAM instances must not write to
  the same state file concurrently.
  """

  alias RedisServerWrapper.{Cli, Cluster, Connection, OSProcess, SecureFile, Sentinel, Server}

  require Logger

  @default_state_file Path.expand("~/.config/redis-server-wrapper/instances.json")
  @connection_option_keys [
    :username,
    :unixsocket,
    :unixsocketperm,
    :tls,
    :tls_port,
    :tls_cert_file,
    :tls_key_file,
    :tls_ca_cert_file,
    :tls_ca_cert_dir,
    :tls_auth_clients,
    :tls_client_cert_file,
    :tls_client_key_file,
    :tls_server_name,
    :tls_insecure
  ]

  @type instance_type :: :basic | :cluster | :sentinel
  @type instance :: %{
          name: String.t(),
          type: instance_type(),
          created_at: String.t(),
          bind: String.t(),
          ports: [non_neg_integer()],
          pids: [non_neg_integer()],
          username: String.t() | nil,
          password: String.t() | nil,
          connection: Connection.t(),
          url: String.t(),
          metadata: map()
        }

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a basic single-node Redis instance.

  ## Options

    * `:name` - instance name (auto-generated if omitted)
    * `:port` - Redis port (default: 6379)
    * `:password` - Redis password (auto-generated if omitted, pass `nil` for no auth)
    * `:username` - optional ACL username paired with `:password`
    * `:bind` - bind address (default: "127.0.0.1")
    * `:control_host` - address used for client and lifecycle operations
    * `:persist` - enable persistence (default: false)
    * `:maxmemory` - memory limit (e.g., "256mb")
    * `:loadmodule` - modules to load; accepts paths or `{path, [args]}` tuples
    * `:distribution` - `:core` (default), `:full`, or `:legacy_stack`
    * TCP, Unix-socket, and TLS options accepted by `RedisServerWrapper.Server`
    * Plus any `RedisServerWrapper.Config` options via `:extra`
  """
  @spec start_basic(keyword()) :: {:ok, instance()} | {:error, term()}
  def start_basic(opts \\ []) do
    with_state_lock(fn -> do_start_basic(opts) end)
  end

  defp do_start_basic(opts) do
    state = load_state()
    name = Keyword.get(opts, :name) || generate_name(state, :basic)
    password = resolve_password(opts)
    port = Keyword.get(opts, :port, 6379)
    bind = Keyword.get(opts, :bind, "127.0.0.1")
    control_host = Keyword.get(opts, :control_host)
    persist = Keyword.get(opts, :persist, false)
    maxmemory = Keyword.get(opts, :maxmemory)
    loadmodule = Keyword.get(opts, :loadmodule, [])
    distribution = Keyword.get(opts, :distribution, :core)
    extra = Keyword.get(opts, :extra, [])

    if Map.has_key?(state.instances, name) do
      {:error, {:instance_exists, name}}
    else
      server_opts =
        [
          port: port,
          bind: bind,
          control_host: control_host,
          password: password,
          save: if(persist, do: :default, else: :disabled),
          appendonly: persist,
          managed: false,
          distribution: distribution,
          loadmodule: loadmodule
        ] ++ server_connection_options(opts)

      server_opts =
        server_opts
        |> maybe_put(:maxmemory, maxmemory)
        |> maybe_put(:extra, if(extra != [], do: extra))

      case Server.start(server_opts) do
        {:ok, pid} ->
          info = Server.info(pid)
          Server.detach(pid)
          Server.stop(pid)

          instance = %{
            name: name,
            type: :basic,
            created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
            bind: info.connection.host || info.host,
            ports: connection_ports(info.connection),
            pids: [info.pid],
            username: info.connection.username,
            password: password,
            connection: info.connection,
            url: Connection.url(info.connection),
            metadata: %{
              persist: persist,
              maxmemory: maxmemory,
              node_dir: info.node_dir
            }
          }

          save_state(put_instance(state, instance))
          print_instance(instance)
          {:ok, instance}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Starts a Redis Cluster.

  ## Options

    * `:name` - instance name (auto-generated if omitted)
    * `:masters` - number of masters (default: 3)
    * `:replicas_per_master` - replicas per master (default: 0)
    * `:base_port` - starting port (default: 7100)
    * `:password` - Redis password (auto-generated if omitted)
    * `:username` - optional ACL username paired with `:password`
    * `:bind` - bind address (default: "127.0.0.1")
    * `:control_host` - address used for client and cluster operations
    * `:loadmodule` - modules loaded into every cluster node
    * `:distribution` - `:core` (default), `:full`, or `:legacy_stack`
    * TLS options accepted by `RedisServerWrapper.Cluster`
  """
  @spec start_cluster(keyword()) :: {:ok, instance()} | {:error, term()}
  def start_cluster(opts \\ []) do
    with_state_lock(fn -> do_start_cluster(opts) end)
  end

  defp do_start_cluster(opts) do
    state = load_state()
    name = Keyword.get(opts, :name) || generate_name(state, :cluster)
    password = resolve_password(opts)
    masters = Keyword.get(opts, :masters, 3)
    replicas = Keyword.get(opts, :replicas_per_master, 0)
    base_port = Keyword.get(opts, :base_port, 7100)
    bind = Keyword.get(opts, :bind, "127.0.0.1")
    control_host = Keyword.get(opts, :control_host)
    loadmodule = Keyword.get(opts, :loadmodule, [])
    distribution = Keyword.get(opts, :distribution, :core)

    if Map.has_key?(state.instances, name) do
      {:error, {:instance_exists, name}}
    else
      cluster_opts =
        [
          masters: masters,
          replicas_per_master: replicas,
          base_port: base_port,
          bind: bind,
          control_host: control_host,
          password: password,
          managed: false,
          distribution: distribution,
          loadmodule: loadmodule
        ] ++ connection_options(opts)

      case Cluster.start(cluster_opts) do
        {:ok, pid} ->
          cluster_info = Cluster.info(pid)
          total_nodes = cluster_info.total_nodes
          ports = Enum.map(0..(total_nodes - 1), &(base_port + &1))

          # Collect OS pids from each node
          os_pids =
            Cluster.nodes(pid)
            |> Enum.map(fn node -> Server.info(node).pid end)

          Cluster.detach(pid)
          Cluster.stop(pid)

          instance = %{
            name: name,
            type: :cluster,
            created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
            bind: cluster_info.control_host,
            ports: ports,
            pids: os_pids,
            username: cluster_info.connection.username,
            password: password,
            connection: cluster_info.connection,
            url: Connection.url(cluster_info.connection),
            metadata: %{
              masters: masters,
              replicas_per_master: replicas
            }
          }

          save_state(put_instance(state, instance))
          print_instance(instance)
          {:ok, instance}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Starts a Redis Sentinel topology.

  ## Options

    * `:name` - instance name (auto-generated if omitted)
    * `:master_port` - master port (default: 6390)
    * `:replicas` - number of replicas (default: 2)
    * `:sentinels` - number of sentinels (default: 3)
    * `:quorum` - Sentinel quorum (default: min(2, sentinels))
    * `:sentinel_base_port` - starting sentinel port (default: 26389)
    * `:password` - Redis password (auto-generated if omitted)
    * `:username` - optional ACL username paired with `:password`
    * `:bind` - bind address (default: "127.0.0.1")
    * `:loadmodule` - modules loaded into the master and every replica
    * `:distribution` - `:core` (default), `:full`, or `:legacy_stack`
    * TLS options accepted by `RedisServerWrapper.Sentinel`
  """
  @spec start_sentinel(keyword()) :: {:ok, instance()} | {:error, term()}
  def start_sentinel(opts \\ []) do
    with_state_lock(fn -> do_start_sentinel(opts) end)
  end

  defp do_start_sentinel(opts) do
    state = load_state()
    name = Keyword.get(opts, :name) || generate_name(state, :sentinel)
    password = resolve_password(opts)
    master_port = Keyword.get(opts, :master_port, 6390)
    num_replicas = Keyword.get(opts, :replicas, 2)
    num_sentinels = Keyword.get(opts, :sentinels, 3)
    quorum = Keyword.get(opts, :quorum, min(2, num_sentinels))
    sentinel_base_port = Keyword.get(opts, :sentinel_base_port, 26_389)
    bind = Keyword.get(opts, :bind, "127.0.0.1")
    control_host = Keyword.get(opts, :control_host)
    loadmodule = Keyword.get(opts, :loadmodule, [])
    distribution = Keyword.get(opts, :distribution, :core)

    if Map.has_key?(state.instances, name) do
      {:error, {:instance_exists, name}}
    else
      sentinel_opts =
        [
          master_port: master_port,
          replicas: num_replicas,
          sentinels: num_sentinels,
          quorum: quorum,
          sentinel_base_port: sentinel_base_port,
          bind: bind,
          control_host: control_host,
          password: password,
          managed: false,
          distribution: distribution,
          loadmodule: loadmodule
        ] ++ connection_options(opts)

      case Sentinel.start(sentinel_opts) do
        {:ok, pid} ->
          sen_info = Sentinel.info(pid)

          # Gather all OS pids: master + replicas are Server GenServers, sentinels are raw
          # We need to inspect the GenServer state for the OS pids
          # For now, collect what we can from the sentinel info
          replica_base_port = master_port + 1

          all_redis_ports =
            [master_port] ++
              ports_from(replica_base_port, num_replicas)

          sentinel_ports = ports_from(sentinel_base_port, num_sentinels)

          # Read OS pids from pidfiles
          redis_pids =
            all_redis_ports
            |> Enum.map(&read_node_pidfile/1)
            |> Enum.reject(&is_nil/1)

          # Also grab sentinel PIDs by checking what's listening on sentinel ports
          sentinel_pids = find_pids_on_ports(sentinel_ports)

          Sentinel.detach(pid)
          Sentinel.stop(pid)

          instance = %{
            name: name,
            type: :sentinel,
            created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
            bind: sen_info.control_host,
            ports: all_redis_ports ++ sentinel_ports,
            pids: redis_pids ++ sentinel_pids,
            username: sen_info.master_connection.username,
            password: password,
            connection: sen_info.master_connection,
            url: Connection.url(sen_info.master_connection),
            metadata: %{
              master_name: sen_info.master_name,
              master_port: master_port,
              replicas: num_replicas,
              sentinels: num_sentinels,
              sentinel_ports: sentinel_ports
            }
          }

          save_state(put_instance(state, instance))
          print_instance(instance)
          {:ok, instance}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Lists all tracked instances.
  Optionally filter by type: `:basic`, `:cluster`, `:sentinel`.
  """
  @spec list(instance_type() | nil) :: [instance()]
  def list(type \\ nil) do
    state = load_state()

    instances =
      state.instances
      |> Map.values()
      |> Enum.sort_by(& &1.created_at)

    instances =
      if type do
        type_str = to_string(type)
        Enum.filter(instances, &(&1.type == type_str || &1.type == type))
      else
        instances
      end

    Enum.each(instances, &print_instance_short/1)
    instances
  end

  @doc """
  Gets detailed info for a named instance, including live status.
  """
  @spec info(String.t()) :: {:ok, map()} | {:error, :not_found}
  def info(name) do
    state = load_state()

    case Map.get(state.instances, name) do
      nil ->
        {:error, :not_found}

      instance ->
        status = check_status(instance)

        result =
          instance
          |> Map.put(:status, status)

        print_instance_detail(result)
        {:ok, result}
    end
  end

  @doc """
  Returns the credentials for a named instance without printing them.

  Manager console output is redacted by default. Use this function when a
  caller explicitly needs the plaintext password or connection URL.
  """
  @spec credentials(String.t()) ::
          {:ok, %{username: String.t() | nil, password: String.t() | nil, url: String.t()}}
          | {:error, :not_found}
  def credentials(name) do
    state = load_state()

    case Map.get(state.instances, name) do
      nil ->
        {:error, :not_found}

      instance ->
        {:ok,
         %{
           username: instance_connection(instance).username,
           password: instance.password,
           url: Connection.url(instance_connection(instance))
         }}
    end
  end

  @doc """
  Stops a named instance by sending SHUTDOWN to all its processes.
  """
  @spec stop(String.t()) :: :ok | {:error, term()}
  def stop(name) do
    with_state_lock(fn -> do_stop(name) end)
  end

  defp do_stop(name) do
    state = load_state()

    case Map.get(state.instances, name) do
      nil ->
        {:error, :not_found}

      instance ->
        case stop_instance_processes(instance) do
          :ok ->
            save_state(remove_instance(state, name))
            IO.puts("Stopped #{name}")
            :ok

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Stops all tracked instances.
  """
  @spec stop_all() :: :ok | {:error, {:instances_not_stopped, map()}}
  def stop_all do
    with_state_lock(&do_stop_all/0)
  end

  defp do_stop_all do
    state = load_state()

    {remaining, failures} =
      Enum.reduce(state.instances, {%{}, %{}}, fn {name, instance}, {remaining, failures} ->
        case stop_instance_processes(instance) do
          :ok ->
            IO.puts("Stopped #{instance.name}")
            {remaining, failures}

          {:error, reason} ->
            {Map.put(remaining, name, instance), Map.put(failures, name, reason)}
        end
      end)

    save_state(%{
      state
      | instances: remaining,
        counters: if(remaining == %{}, do: %{}, else: state.counters)
    })

    if failures == %{} do
      :ok
    else
      {:error, {:instances_not_stopped, failures}}
    end
  end

  @doc """
  Removes instances from state that are no longer running.
  """
  @spec cleanup() :: {non_neg_integer(), non_neg_integer()}
  def cleanup do
    with_state_lock(&do_cleanup/0)
  end

  defp do_cleanup do
    state = load_state()

    {running, dead} =
      state.instances
      |> Map.values()
      |> Enum.split_with(fn instance -> check_status(instance) == :running end)

    Enum.each(dead, fn instance ->
      IO.puts("Removing dead instance: #{instance.name}")
    end)

    new_instances = Map.new(running, &{&1.name, &1})
    save_state(%{state | instances: new_instances})

    {length(running), length(dead)}
  end

  # -------------------------------------------------------------------
  # State persistence
  # -------------------------------------------------------------------

  defp load_state do
    state_file = state_file()
    ensure_state_directory!(state_file)

    case SecureFile.harden_private_file(state_file) do
      :ok ->
        read_state_file(state_file)

      :missing ->
        empty_state()

      {:error, reason} ->
        recover_corrupt_state(state_file, reason)
    end
  end

  defp save_state(state) do
    state_file = state_file()
    ensure_state_directory!(state_file)
    json = encode_json(serialize_state(state))
    SecureFile.atomic_write_private!(state_file, json)
    state
  end

  defp read_state_file(state_file) do
    case File.read(state_file) do
      {:ok, ""} ->
        empty_state()

      {:ok, content} ->
        decode_state(state_file, content)

      {:error, :enoent} ->
        empty_state()

      {:error, reason} ->
        recover_corrupt_state(state_file, reason)
    end
  end

  defp decode_state(state_file, content) do
    case JSON.decode(content) do
      {:ok, data} ->
        deserialize_state(data)

      {:error, reason} ->
        recover_corrupt_state(state_file, reason)
    end
  rescue
    error -> recover_corrupt_state(state_file, Exception.message(error))
  end

  defp recover_corrupt_state(state_file, reason) do
    backup = "#{state_file}.corrupt-#{System.unique_integer([:positive, :monotonic])}"

    case File.rename(state_file, backup) do
      :ok ->
        Logger.error(
          "Manager state was invalid and has been preserved at #{backup}: #{inspect(reason)}"
        )

      {:error, :enoent} ->
        Logger.warning("Manager state disappeared while recovering invalid data")

      {:error, rename_reason} ->
        raise File.Error,
          reason: rename_reason,
          action: "preserve invalid Manager state",
          path: state_file
    end

    empty_state()
  end

  defp ensure_state_directory!(state_file) do
    directory = Path.dirname(state_file)
    directory_existed? = File.dir?(directory)
    File.mkdir_p!(directory)

    if not directory_existed? or Path.expand(state_file) == Path.expand(@default_state_file) do
      File.chmod!(directory, 0o700)
    end
  end

  defp state_file do
    Application.get_env(:redis_server_wrapper, :manager_state_file, @default_state_file)
  end

  defp with_state_lock(fun) do
    resource = {__MODULE__, Path.expand(state_file())}
    :global.trans({resource, self()}, fun)
  end

  defp empty_state, do: %{instances: %{}, counters: %{}}

  defp serialize_state(state) do
    instances =
      Map.new(state.instances, fn {name, instance} ->
        {name, Map.update!(instance, :type, &to_string/1)}
      end)

    %{"instances" => instances, "counters" => state.counters}
  end

  defp deserialize_state(%{"instances" => instances} = data) when is_map(instances) do
    instances =
      Map.new(instances, fn {name, inst} -> {name, deserialize_instance(name, inst)} end)

    counters = data["counters"] || %{}

    if is_map(counters) do
      %{instances: instances, counters: counters}
    else
      raise ArgumentError, "Manager state counters must be an object"
    end
  end

  defp deserialize_state(%{} = data) when map_size(data) == 0, do: empty_state()
  defp deserialize_state(_data), do: raise(ArgumentError, "Manager state must be an object")

  defp deserialize_instance(name, inst) when is_map(inst) do
    connection = deserialize_connection(inst)

    %{
      name: inst["name"] || name,
      type: deserialize_type(inst["type"] || "basic"),
      created_at: inst["created_at"],
      bind: inst["bind"] || "127.0.0.1",
      ports: inst["ports"] || [],
      pids: inst["pids"] || [],
      username: inst["username"] || connection.username,
      password: inst["password"],
      connection: connection,
      url: inst["url"] || "",
      metadata: atomize_keys(inst["metadata"] || %{})
    }
  end

  defp deserialize_instance(_name, _inst),
    do: raise(ArgumentError, "Manager instance state must be an object")

  defp deserialize_type("basic"), do: :basic
  defp deserialize_type("cluster"), do: :cluster
  defp deserialize_type("sentinel"), do: :sentinel
  defp deserialize_type(type), do: raise(ArgumentError, "unknown Manager instance type: #{type}")

  defp deserialize_connection(%{"connection" => connection}) when is_map(connection) do
    Connection.from_map(connection)
  end

  defp deserialize_connection(instance) do
    port =
      case instance["ports"] || [] do
        [port | _rest] when is_integer(port) and port > 0 -> port
        _other -> 6379
      end

    Connection.new(
      host: instance["bind"] || "127.0.0.1",
      port: port,
      username: instance["username"],
      password: instance["password"]
    )
  end

  defp atomize_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {existing_atom_or_string(k), atomize_keys(v)}
      {k, v} -> {k, atomize_keys(v)}
    end)
  end

  defp atomize_keys(list) when is_list(list), do: Enum.map(list, &atomize_keys/1)
  defp atomize_keys(value), do: value

  defp existing_atom_or_string(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp encode_json(data) do
    JSON.encode!(data)
  end

  defp put_instance(state, instance) do
    type_key = to_string(instance.type)
    counter = Map.get(state.counters, type_key, 0)

    %{
      state
      | instances: Map.put(state.instances, instance.name, instance),
        counters: Map.put(state.counters, type_key, max(counter, extract_counter(instance.name)))
    }
  end

  defp remove_instance(state, name) do
    %{state | instances: Map.delete(state.instances, name)}
  end

  # -------------------------------------------------------------------
  # Name & password generation
  # -------------------------------------------------------------------

  defp generate_name(state, type) do
    type_str = to_string(type)
    counter = Map.get(state.counters, type_str, 0) + 1
    "redis-#{type_str}-#{counter}"
  end

  defp extract_counter(name) do
    case Regex.run(~r/-(\d+)$/, name) do
      [_, n] -> String.to_integer(n)
      _ -> 0
    end
  end

  # If :password key is explicitly present (even as nil), use that value.
  # If omitted entirely, auto-generate.
  defp resolve_password(opts) do
    if Keyword.has_key?(opts, :password) do
      Keyword.get(opts, :password)
    else
      generate_password()
    end
  end

  defp generate_password(length \\ 16) do
    :crypto.strong_rand_bytes(length)
    |> Base.url_encode64(padding: false)
    |> binary_part(0, length)
  end

  # -------------------------------------------------------------------
  # Process management
  # -------------------------------------------------------------------

  defp stop_instance_processes(instance) do
    case owned_listeners(instance) do
      {:ok, listeners} -> stop_owned_instance(instance, listeners)
      {:error, _reason} = error -> error
    end
  end

  defp stop_owned_instance(instance, listeners) when map_size(listeners) == 0 do
    if Enum.any?(instance.pids, &OSProcess.alive?/1) do
      {:error, {:process_ownership_not_verified, instance.name, instance.pids}}
    else
      :ok
    end
  end

  defp stop_owned_instance(instance, listeners) do
    gracefully_stop_owned_listeners(instance, listeners)
    Process.sleep(1000)

    with :ok <- signal_owned_listeners(instance, :term),
         _ <- Process.sleep(500),
         :ok <- signal_owned_listeners(instance, :kill) do
      verify_instance_stopped(instance)
    end
  end

  defp verify_instance_stopped(instance) do
    case Enum.filter(instance.pids, &OSProcess.alive?/1) do
      [] -> :ok
      remaining -> {:error, {:processes_still_running, instance.name, remaining}}
    end
  end

  defp owned_listeners(instance) do
    Enum.reduce_while(
      listener_targets(instance),
      {:ok, %{}},
      &collect_owned_listener(&1, &2, instance)
    )
  end

  defp collect_owned_listener(target, {:ok, listeners}, instance) do
    case pids_on_target(target) do
      {:ok, target_pids} ->
        owned_pids = Enum.filter(target_pids, &(&1 in instance.pids))
        {:cont, {:ok, maybe_put_listener(listeners, target, owned_pids)}}

      {:error, reason} ->
        {:halt, {:error, {:process_ownership_not_verified, instance.name, reason}}}
    end
  end

  defp maybe_put_listener(listeners, _target, []), do: listeners
  defp maybe_put_listener(listeners, target, pids), do: Map.put(listeners, target, pids)

  defp gracefully_stop_owned_listeners(instance, listeners) do
    Enum.each(Map.keys(listeners), fn target ->
      cli = Cli.new(connection: connection_for_target(instance, target))
      _result = Cli.run(cli, ["SHUTDOWN", "NOSAVE"])
    end)
  end

  defp listener_targets(instance) do
    case instance_connection(instance) do
      %Connection{transport: :unix, socket: socket} -> [{:unix, socket}]
      _connection -> Enum.map(instance.ports, &{:tcp, &1})
    end
  end

  defp pids_on_target({:tcp, port}), do: OSProcess.pids_on_port(port)
  defp pids_on_target({:unix, socket}), do: OSProcess.pids_on_socket(socket)

  defp connection_for_target(instance, {:tcp, port}) do
    instance |> instance_connection() |> Connection.with_port(port)
  end

  defp connection_for_target(instance, {:unix, _socket}), do: instance_connection(instance)

  defp instance_connection(%{connection: %Connection{} = connection}), do: connection

  defp instance_connection(instance) do
    port = Enum.find(instance.ports, 6379, &(&1 > 0))

    Connection.new(
      host: instance.bind,
      port: port,
      username: Map.get(instance, :username),
      password: instance.password
    )
  end

  defp signal_owned_listeners(instance, signal) do
    with {:ok, listeners} <- owned_listeners(instance) do
      listeners
      |> Map.values()
      |> List.flatten()
      |> Enum.uniq()
      |> Enum.reduce_while(:ok, &signal_listener(&1, signal, &2))
    end
  end

  defp signal_listener(pid, signal, :ok) do
    case OSProcess.signal(pid, signal) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp check_status(instance) do
    if Enum.any?(instance.pids, &OSProcess.alive?/1) do
      :running
    else
      :stopped
    end
  end

  defp find_pids_on_ports(ports) do
    unless OSProcess.available?("lsof") do
      Logger.warning("lsof is unavailable; sentinel OS PIDs will not be recorded by port")
    end

    Enum.flat_map(ports, fn port ->
      case OSProcess.pids_on_port(port) do
        {:ok, pids} -> pids
        {:error, {:executable_not_found, "lsof"}} -> []
      end
    end)
    |> Enum.uniq()
  end

  defp read_node_pidfile(port) do
    pidfile =
      Path.join([System.tmp_dir!(), "redis-server-wrapper", "node-#{port}", "redis.pid"])

    read_pidfile(pidfile)
  end

  defp read_pidfile(path) do
    case File.read(path) do
      {:ok, content} -> content |> String.trim() |> String.to_integer()
      {:error, _} -> nil
    end
  end

  defp ports_from(_base_port, 0), do: []
  defp ports_from(base_port, count), do: Enum.map(0..(count - 1), &(base_port + &1))

  defp connection_ports(%Connection{transport: :unix}), do: []
  defp connection_ports(%Connection{port: port}), do: [port]

  # -------------------------------------------------------------------
  # Display helpers
  # -------------------------------------------------------------------

  defp print_instance(instance) do
    IO.puts("")
    IO.puts("  #{instance.name} (#{instance.type})")
    IO.puts("  URL: #{redact_url(instance.url)}")
    IO.puts("  Ports: #{Enum.join(instance.ports, ", ")}")
    IO.puts("  PIDs: #{Enum.join(instance.pids, ", ")}")

    if instance.password do
      IO.puts("  Password: [REDACTED]")
    end

    IO.puts("")
  end

  defp print_instance_short(instance) do
    status = check_status(instance)
    status_str = if status == :running, do: "running", else: "stopped"

    IO.puts(
      "  #{instance.name}\t#{instance.type}\t#{status_str}\t#{Enum.join(instance.ports, ",")}"
    )
  end

  defp print_instance_detail(instance) do
    IO.puts("")
    IO.puts("  #{instance.name}")
    IO.puts("  Type:     #{instance.type}")
    IO.puts("  Status:   #{instance.status}")
    IO.puts("  URL:      #{redact_url(instance.url)}")
    IO.puts("  Ports:    #{Enum.join(instance.ports, ", ")}")
    IO.puts("  PIDs:     #{Enum.join(instance.pids, ", ")}")
    IO.puts("  Created:  #{instance.created_at}")

    if instance.password do
      IO.puts("  Password: [REDACTED]")
    end

    if map_size(instance.metadata) > 0 do
      IO.puts("  Metadata: #{inspect(instance.metadata)}")
    end

    IO.puts("")
  end

  defp redact_url(url) do
    String.replace(url, ~r/\A(redis(?:s)?:\/\/)[^@]+@/, "\\1[REDACTED]@")
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp connection_options(opts), do: Keyword.take(opts, @connection_option_keys)

  defp server_connection_options(opts) do
    opts
    |> connection_options()
    |> Keyword.delete(:tls)
  end
end
