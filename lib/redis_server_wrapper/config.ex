defmodule RedisServerWrapper.Config do
  # Redis has many configuration directives; a flat struct is the clearest mapping.
  # credo:disable-for-this-file Credo.Check.Warning.StructFieldAmount
  @moduledoc """
  Redis server configuration builder.

  Generates redis.conf content from a structured configuration.
  Supports all common Redis directives with an escape hatch via `:extra` for anything else.
  """

  @type log_level :: :debug | :verbose | :notice | :warning
  @type append_fsync :: :always | :everysec | :no
  @type save_policy :: :disabled | :default | [{pos_integer(), pos_integer()}]

  @type t :: %__MODULE__{
          port: non_neg_integer(),
          bind: String.t(),
          password: String.t() | nil,
          loglevel: log_level(),
          logfile: String.t() | nil,
          daemonize: boolean(),
          pidfile: String.t() | nil,
          dir: String.t() | nil,
          # Persistence
          save: save_policy(),
          appendonly: boolean(),
          appendfsync: append_fsync(),
          # Memory
          maxmemory: String.t() | nil,
          maxmemory_policy: String.t() | nil,
          # Network
          tcp_backlog: non_neg_integer() | nil,
          timeout: non_neg_integer() | nil,
          tcp_keepalive: non_neg_integer() | nil,
          unixsocket: String.t() | nil,
          unixsocketperm: String.t() | nil,
          # TLS
          tls_port: non_neg_integer() | nil,
          tls_cert_file: String.t() | nil,
          tls_key_file: String.t() | nil,
          tls_ca_cert_file: String.t() | nil,
          tls_auth_clients: String.t() | nil,
          # Replication
          replicaof: {String.t(), non_neg_integer()} | nil,
          masterauth: String.t() | nil,
          # Cluster
          cluster_enabled: boolean(),
          cluster_config_file: String.t() | nil,
          cluster_node_timeout: non_neg_integer() | nil,
          cluster_announce_hostname: String.t() | nil,
          cluster_announce_port: non_neg_integer() | nil,
          cluster_announce_bus_port: non_neg_integer() | nil,
          # Modules
          loadmodule: [String.t()],
          # Catch-all
          extra: [{String.t(), String.t()}]
        }

  defstruct port: 6379,
            bind: "127.0.0.1",
            password: nil,
            loglevel: :notice,
            logfile: nil,
            daemonize: false,
            pidfile: nil,
            dir: nil,
            save: :default,
            appendonly: false,
            appendfsync: :everysec,
            maxmemory: nil,
            maxmemory_policy: nil,
            tcp_backlog: nil,
            timeout: nil,
            tcp_keepalive: nil,
            unixsocket: nil,
            unixsocketperm: nil,
            tls_port: nil,
            tls_cert_file: nil,
            tls_key_file: nil,
            tls_ca_cert_file: nil,
            tls_auth_clients: nil,
            replicaof: nil,
            masterauth: nil,
            cluster_enabled: false,
            cluster_config_file: nil,
            cluster_node_timeout: nil,
            cluster_announce_hostname: nil,
            cluster_announce_port: nil,
            cluster_announce_bus_port: nil,
            loadmodule: [],
            extra: []

  @doc """
  Creates a new config from keyword options.

      Config.new(port: 6400, password: "secret", appendonly: true)
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    struct!(__MODULE__, opts)
  end

  @doc """
  Generates redis.conf file content from the config.
  """
  @spec to_config_string(t()) :: String.t()
  def to_config_string(%__MODULE__{} = config) do
    []
    |> emit("port", config.port)
    |> emit("bind", config.bind)
    |> emit_if("requirepass", config.password)
    |> emit("loglevel", config.loglevel)
    |> emit_if("logfile", config.logfile)
    |> emit("daemonize", yn(config.daemonize))
    |> emit_if("pidfile", config.pidfile)
    |> emit_if("dir", config.dir)
    |> emit_save(config.save)
    |> emit("appendonly", yn(config.appendonly))
    |> emit("appendfsync", config.appendfsync)
    |> emit_if("maxmemory", config.maxmemory)
    |> emit_if("maxmemory-policy", config.maxmemory_policy)
    |> emit_if("tcp-backlog", config.tcp_backlog)
    |> emit_if("timeout", config.timeout)
    |> emit_if("tcp-keepalive", config.tcp_keepalive)
    |> emit_if("unixsocket", config.unixsocket)
    |> emit_if("unixsocketperm", config.unixsocketperm)
    |> emit_tls(config)
    |> emit_replication(config)
    |> emit_cluster(config)
    |> emit_modules(config.loadmodule)
    |> emit_extra(config.extra)
    |> Enum.reverse()
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  # Directive emitters

  defp emit(acc, key, value), do: ["#{key} #{value}" | acc]

  defp emit_if(acc, _key, nil), do: acc
  defp emit_if(acc, key, value), do: emit(acc, key, value)

  defp emit_save(acc, :default), do: acc
  defp emit_save(acc, :disabled), do: ["save \"\"" | acc]

  defp emit_save(acc, policies) when is_list(policies) do
    Enum.reduce(policies, acc, fn {seconds, changes}, acc ->
      ["save #{seconds} #{changes}" | acc]
    end)
  end

  defp emit_tls(acc, %{tls_port: nil}), do: acc

  defp emit_tls(acc, config) do
    acc
    |> emit("tls-port", config.tls_port)
    |> emit_if("tls-cert-file", config.tls_cert_file)
    |> emit_if("tls-key-file", config.tls_key_file)
    |> emit_if("tls-ca-cert-file", config.tls_ca_cert_file)
    |> emit_if("tls-auth-clients", config.tls_auth_clients)
  end

  defp emit_replication(acc, %{replicaof: nil}), do: acc

  defp emit_replication(acc, config) do
    {host, port} = config.replicaof

    acc
    |> emit("replicaof", "#{host} #{port}")
    |> emit_if("masterauth", config.masterauth)
  end

  defp emit_cluster(acc, %{cluster_enabled: false}), do: acc

  defp emit_cluster(acc, config) do
    acc
    |> emit("cluster-enabled", "yes")
    |> emit_if("cluster-config-file", config.cluster_config_file)
    |> emit_if("cluster-node-timeout", config.cluster_node_timeout)
    |> emit_if("cluster-announce-hostname", config.cluster_announce_hostname)
    |> emit_if("cluster-announce-port", config.cluster_announce_port)
    |> emit_if("cluster-announce-bus-port", config.cluster_announce_bus_port)
  end

  defp emit_modules(acc, []), do: acc

  defp emit_modules(acc, modules) do
    Enum.reduce(modules, acc, fn mod, acc -> ["loadmodule #{mod}" | acc] end)
  end

  defp emit_extra(acc, []), do: acc

  defp emit_extra(acc, extras) do
    Enum.reduce(extras, acc, fn {key, value}, acc -> ["#{key} #{value}" | acc] end)
  end

  defp yn(true), do: "yes"
  defp yn(false), do: "no"
end
