defmodule RedisServerWrapper.Manager do
  @moduledoc """
  Persistent instance manager for Redis server processes.

  Tracks instances across IEx sessions via a JSON state file. Instances are
  detached by default so they outlive the Elixir process that started them.

  ## Usage

      Manager.start_basic(port: 6400)
      Manager.start_cluster(masters: 3, base_port: 7000)
      Manager.start_sentinel(master_port: 6390)

      Manager.list()
      Manager.info("redis-basic-1")
      Manager.stop("redis-basic-1")
      Manager.cleanup()

  State is stored at `~/.config/redis-server-wrapper/instances.json`.
  """

  alias RedisServerWrapper.{Cli, Server, Cluster, Sentinel}

  require Logger

  @config_dir Path.expand("~/.config/redis-server-wrapper")
  @state_file Path.join(@config_dir, "instances.json")

  @type instance_type :: :basic | :cluster | :sentinel
  @type instance :: %{
          name: String.t(),
          type: instance_type(),
          created_at: String.t(),
          bind: String.t(),
          ports: [non_neg_integer()],
          pids: [non_neg_integer()],
          password: String.t() | nil,
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
    * `:bind` - bind address (default: "127.0.0.1")
    * `:persist` - enable persistence (default: false)
    * `:maxmemory` - memory limit (e.g., "256mb")
    * Plus any `RedisServerWrapper.Config` options via `:extra`
  """
  @spec start_basic(keyword()) :: {:ok, instance()} | {:error, term()}
  def start_basic(opts \\ []) do
    state = load_state()
    name = Keyword.get(opts, :name) || generate_name(state, :basic)
    password = resolve_password(opts)
    port = Keyword.get(opts, :port, 6379)
    bind = Keyword.get(opts, :bind, "127.0.0.1")
    persist = Keyword.get(opts, :persist, false)
    maxmemory = Keyword.get(opts, :maxmemory)
    extra = Keyword.get(opts, :extra, [])

    if Map.has_key?(state.instances, name) do
      {:error, {:instance_exists, name}}
    else
      server_opts =
        [
          port: port,
          bind: bind,
          password: password,
          save: if(persist, do: :default, else: :disabled),
          appendonly: persist
        ]
        |> maybe_put(:maxmemory, maxmemory)
        |> maybe_put(:extra, if(extra != [], do: extra))

      case Server.start_link(server_opts) do
        {:ok, pid} ->
          info = Server.info(pid)
          Server.detach(pid)
          Server.stop(pid)

          instance = %{
            name: name,
            type: :basic,
            created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
            bind: bind,
            ports: [port],
            pids: [info.pid],
            password: password,
            url: build_url(bind, port, password),
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
    * `:base_port` - starting port (default: 7000)
    * `:password` - Redis password (auto-generated if omitted)
    * `:bind` - bind address (default: "127.0.0.1")
  """
  @spec start_cluster(keyword()) :: {:ok, instance()} | {:error, term()}
  def start_cluster(opts \\ []) do
    state = load_state()
    name = Keyword.get(opts, :name) || generate_name(state, :cluster)
    password = resolve_password(opts)
    masters = Keyword.get(opts, :masters, 3)
    replicas = Keyword.get(opts, :replicas_per_master, 0)
    base_port = Keyword.get(opts, :base_port, 7000)
    bind = Keyword.get(opts, :bind, "127.0.0.1")

    if Map.has_key?(state.instances, name) do
      {:error, {:instance_exists, name}}
    else
      cluster_opts = [
        masters: masters,
        replicas_per_master: replicas,
        base_port: base_port,
        bind: bind,
        password: password
      ]

      case Cluster.start_link(cluster_opts) do
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
            bind: bind,
            ports: ports,
            pids: os_pids,
            password: password,
            url: build_url(bind, base_port, password),
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
    * `:sentinel_base_port` - starting sentinel port (default: 26389)
    * `:password` - Redis password (auto-generated if omitted)
    * `:bind` - bind address (default: "127.0.0.1")
  """
  @spec start_sentinel(keyword()) :: {:ok, instance()} | {:error, term()}
  def start_sentinel(opts \\ []) do
    state = load_state()
    name = Keyword.get(opts, :name) || generate_name(state, :sentinel)
    password = resolve_password(opts)
    master_port = Keyword.get(opts, :master_port, 6390)
    num_replicas = Keyword.get(opts, :replicas, 2)
    num_sentinels = Keyword.get(opts, :sentinels, 3)
    sentinel_base_port = Keyword.get(opts, :sentinel_base_port, 26389)
    bind = Keyword.get(opts, :bind, "127.0.0.1")

    if Map.has_key?(state.instances, name) do
      {:error, {:instance_exists, name}}
    else
      sentinel_opts = [
        master_port: master_port,
        replicas: num_replicas,
        sentinels: num_sentinels,
        sentinel_base_port: sentinel_base_port,
        bind: bind,
        password: password
      ]

      case Sentinel.start_link(sentinel_opts) do
        {:ok, pid} ->
          sen_info = Sentinel.info(pid)

          # Gather all OS pids: master + replicas are Server GenServers, sentinels are raw
          # We need to inspect the GenServer state for the OS pids
          # For now, collect what we can from the sentinel info
          replica_base_port = master_port + 1

          all_redis_ports =
            [master_port] ++
              Enum.map(0..(num_replicas - 1), &(replica_base_port + &1))

          sentinel_ports = Enum.map(0..(num_sentinels - 1), &(sentinel_base_port + &1))

          # Read OS pids from pidfiles
          all_pids =
            (all_redis_ports
             |> Enum.map(fn port ->
               pidfile =
                 Path.join([
                   System.tmp_dir!(),
                   "redis-server-wrapper",
                   "node-#{port}",
                   "redis.pid"
                 ])

               read_pidfile(pidfile)
             end)) ++
              (sentinel_ports
               |> Enum.flat_map(fn _port ->
                 # Sentinel pidfiles are in timestamped dirs, so we rely on
                 # find_pids_on_ports below instead
                 []
               end))

          all_pids = Enum.reject(all_pids, &is_nil/1)

          # Also grab sentinel PIDs by checking what's listening on sentinel ports
          sentinel_pids = find_pids_on_ports(sentinel_ports)

          Sentinel.detach(pid)
          Sentinel.stop(pid)

          instance = %{
            name: name,
            type: :sentinel,
            created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
            bind: bind,
            ports: all_redis_ports ++ sentinel_ports,
            pids: all_pids ++ sentinel_pids,
            password: password,
            url: build_url(bind, master_port, password),
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
  Stops a named instance by sending SHUTDOWN to all its processes.
  """
  @spec stop(String.t()) :: :ok | {:error, :not_found}
  def stop(name) do
    state = load_state()

    case Map.get(state.instances, name) do
      nil ->
        {:error, :not_found}

      instance ->
        stop_instance_processes(instance)
        save_state(remove_instance(state, name))
        IO.puts("Stopped #{name}")
        :ok
    end
  end

  @doc """
  Stops all tracked instances.
  """
  @spec stop_all() :: :ok
  def stop_all do
    state = load_state()

    state.instances
    |> Map.values()
    |> Enum.each(fn instance ->
      stop_instance_processes(instance)
      IO.puts("Stopped #{instance.name}")
    end)

    save_state(%{state | instances: %{}, counters: %{}})
    :ok
  end

  @doc """
  Removes instances from state that are no longer running.
  """
  @spec cleanup() :: {non_neg_integer(), non_neg_integer()}
  def cleanup do
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
    File.mkdir_p!(@config_dir)

    case File.read(@state_file) do
      {:ok, content} when byte_size(content) > 0 ->
        data = JSON.decode!(content)
        deserialize_state(data)

      _ ->
        empty_state()
    end
  end

  defp save_state(state) do
    File.mkdir_p!(@config_dir)
    json = encode_json(serialize_state(state))
    File.write!(@state_file, json)
    state
  end

  defp empty_state, do: %{instances: %{}, counters: %{}}

  defp serialize_state(state) do
    instances =
      Map.new(state.instances, fn {name, instance} ->
        {name, Map.update!(instance, :type, &to_string/1)}
      end)

    %{"instances" => instances, "counters" => state.counters}
  end

  defp deserialize_state(data) do
    instances =
      (data["instances"] || %{})
      |> Map.new(fn {name, inst} ->
        instance = %{
          name: inst["name"] || name,
          type: String.to_existing_atom(inst["type"] || "basic"),
          created_at: inst["created_at"],
          bind: inst["bind"] || "127.0.0.1",
          ports: inst["ports"] || [],
          pids: inst["pids"] || [],
          password: inst["password"],
          url: inst["url"] || "",
          metadata: atomize_keys(inst["metadata"] || %{})
        }

        {name, instance}
      end)

    counters =
      (data["counters"] || %{})
      |> Map.new(fn {k, v} -> {k, v} end)

    %{instances: instances, counters: counters}
  end

  defp atomize_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {String.to_atom(k), atomize_keys(v)}
      {k, v} -> {k, atomize_keys(v)}
    end)
  end

  defp atomize_keys(list) when is_list(list), do: Enum.map(list, &atomize_keys/1)
  defp atomize_keys(value), do: value

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

  @password_chars ~c"bcdfghjkmnpqrstvwxyz23456789BCDFGHJKMNPQRSTVWXYZ"
  defp generate_password(length \\ 16) do
    for _ <- 1..length, into: "" do
      <<Enum.random(@password_chars)>>
    end
  end

  # -------------------------------------------------------------------
  # Process management
  # -------------------------------------------------------------------

  defp stop_instance_processes(instance) do
    # First try graceful SHUTDOWN via redis-cli on each port
    Enum.each(instance.ports, fn port ->
      cli = Cli.new(host: instance.bind, port: port, password: instance.password)
      Cli.shutdown(cli)
    end)

    Process.sleep(1000)

    # Then SIGTERM/SIGKILL any remaining PIDs
    Enum.each(instance.pids, fn pid ->
      if pid_alive?(pid) do
        System.cmd("kill", ["-TERM", to_string(pid)], stderr_to_stdout: true)
      end
    end)

    Process.sleep(500)

    Enum.each(instance.pids, fn pid ->
      if pid_alive?(pid) do
        System.cmd("kill", ["-9", to_string(pid)], stderr_to_stdout: true)
      end
    end)
  end

  defp check_status(instance) do
    if Enum.any?(instance.pids, &pid_alive?/1) do
      :running
    else
      :stopped
    end
  end

  defp pid_alive?(pid) when is_integer(pid) do
    case System.cmd("kill", ["-0", to_string(pid)], stderr_to_stdout: true) do
      {_, 0} -> true
      _ -> false
    end
  end

  defp pid_alive?(_), do: false

  defp find_pids_on_ports(ports) do
    Enum.flat_map(ports, fn port ->
      case System.cmd("lsof", ["-ti", ":#{port}"], stderr_to_stdout: true) do
        {output, 0} ->
          output
          |> String.split(~r/\s+/, trim: true)
          |> Enum.map(&String.to_integer/1)

        _ ->
          []
      end
    end)
    |> Enum.uniq()
  end

  defp read_pidfile(path) do
    case File.read(path) do
      {:ok, content} -> content |> String.trim() |> String.to_integer()
      {:error, _} -> nil
    end
  end

  # -------------------------------------------------------------------
  # URL building
  # -------------------------------------------------------------------

  defp build_url(host, port, nil), do: "redis://#{host}:#{port}"
  defp build_url(host, port, password), do: "redis://:#{password}@#{host}:#{port}"

  # -------------------------------------------------------------------
  # Display helpers
  # -------------------------------------------------------------------

  defp print_instance(instance) do
    IO.puts("")
    IO.puts("  #{instance.name} (#{instance.type})")
    IO.puts("  URL: #{instance.url}")
    IO.puts("  Ports: #{Enum.join(instance.ports, ", ")}")
    IO.puts("  PIDs: #{Enum.join(instance.pids, ", ")}")

    if instance.password do
      IO.puts("  Password: #{instance.password}")
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
    IO.puts("  URL:      #{instance.url}")
    IO.puts("  Ports:    #{Enum.join(instance.ports, ", ")}")
    IO.puts("  PIDs:     #{Enum.join(instance.pids, ", ")}")
    IO.puts("  Created:  #{instance.created_at}")

    if instance.password do
      IO.puts("  Password: #{instance.password}")
    end

    if map_size(instance.metadata) > 0 do
      IO.puts("  Metadata: #{inspect(instance.metadata)}")
    end

    IO.puts("")
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
