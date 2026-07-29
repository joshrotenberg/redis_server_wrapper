defmodule RedisServerWrapper.Config do
  # Redis has many configuration directives; a flat struct is the clearest mapping.
  # credo:disable-for-this-file Credo.Check.Warning.StructFieldAmount
  @moduledoc """
  Redis server configuration builder.

  Generates redis.conf content from a structured configuration.
  Supports common Redis directives plus token-safe `:extra` directives for
  anything else. Every value is encoded as a Redis configuration token.
  """

  @type log_level :: :debug | :verbose | :notice | :warning
  @type append_fsync :: :always | :everysec | :no
  @type save_policy :: :disabled | :default | [{pos_integer(), pos_integer()}]
  @typedoc """
  A Redis module path, optionally paired with an argument vector.

  A plain string is one module-path token. Use `{path, args}` when arguments
  are needed; paths and arguments are always quoted safely when required.
  """
  @type module_spec :: String.t() | {String.t(), [String.t()]}
  @type bind_addresses :: String.t() | [String.t()]
  @type extra_spec :: {String.t(), String.t() | [String.t()]}

  alias RedisServerWrapper.Connection

  # These validators intentionally defend the runtime struct boundary against
  # values outside the public typespec.
  @dialyzer {:nowarn_function, validate_save!: 1}
  @dialyzer {:nowarn_function, validate_modules!: 1}
  @dialyzer {:nowarn_function, validate_extra!: 1}

  @type t :: %__MODULE__{
          port: non_neg_integer(),
          bind: bind_addresses(),
          control_host: String.t() | nil,
          username: String.t() | nil,
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
          tls_ca_cert_dir: String.t() | nil,
          tls_auth_clients: String.t() | nil,
          tls_client_cert_file: String.t() | nil,
          tls_client_key_file: String.t() | nil,
          tls_server_name: String.t() | nil,
          tls_insecure: boolean(),
          tls_replication: boolean(),
          tls_cluster: boolean(),
          # Replication
          replicaof: {String.t(), non_neg_integer()} | nil,
          masteruser: String.t() | nil,
          masterauth: String.t() | nil,
          # Cluster
          cluster_enabled: boolean(),
          cluster_config_file: String.t() | nil,
          cluster_node_timeout: non_neg_integer() | nil,
          cluster_announce_hostname: String.t() | nil,
          cluster_announce_port: non_neg_integer() | nil,
          cluster_announce_bus_port: non_neg_integer() | nil,
          # Modules
          loadmodule: [module_spec()],
          # Catch-all
          extra: [extra_spec()]
        }

  defstruct port: 6379,
            bind: "127.0.0.1",
            control_host: nil,
            username: nil,
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
            tls_ca_cert_dir: nil,
            tls_auth_clients: nil,
            tls_client_cert_file: nil,
            tls_client_key_file: nil,
            tls_server_name: nil,
            tls_insecure: false,
            tls_replication: false,
            tls_cluster: false,
            replicaof: nil,
            masteruser: nil,
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

      Config.new(
        loadmodule: [
          {"/path/to/module.so", ["events", "expired,set", "maxlen", "10000"]}
        ]
      )
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = struct!(__MODULE__, opts)
    validate!(config)
    config
  end

  @doc """
  Returns the address used by `redis-cli` and lifecycle probes.

  `:bind` controls Redis listen addresses. Set `:control_host` when Redis
  listens on multiple addresses or on a wildcard address.
  """
  @spec control_host(t()) :: String.t()
  def control_host(%__MODULE__{control_host: host}) when is_binary(host), do: host

  def control_host(%__MODULE__{bind: bind}) do
    bind
    |> bind_tokens()
    |> List.first()
    |> normalize_control_host()
  end

  @doc "Returns normalized Redis listen-address tokens."
  @spec listen_addresses(t()) :: [String.t()]
  def listen_addresses(%__MODULE__{bind: bind}), do: bind_tokens(bind)

  @doc "Builds the client connection used to control this Redis server."
  @spec connection(t()) :: Connection.t()
  def connection(%__MODULE__{} = config) do
    common = [
      username: config.username,
      password: config.password
    ]

    cond do
      is_integer(config.tls_port) and config.tls_port > 0 ->
        Connection.new(
          [
            transport: :tls,
            host: control_host(config),
            port: config.tls_port,
            tls_ca_cert_file: config.tls_ca_cert_file,
            tls_ca_cert_dir: config.tls_ca_cert_dir,
            tls_client_cert_file: config.tls_client_cert_file,
            tls_client_key_file: config.tls_client_key_file,
            tls_server_name: config.tls_server_name,
            tls_insecure: config.tls_insecure
          ] ++ common
        )

      is_integer(config.port) and config.port > 0 ->
        Connection.new([transport: :tcp, host: control_host(config), port: config.port] ++ common)

      is_binary(config.unixsocket) ->
        Connection.new([transport: :unix, socket: config.unixsocket] ++ common)
    end
  end

  @doc """
  Encodes one Redis configuration directive from an argument vector.

  This is also used by the Sentinel configuration generator so it follows the
  same quoting and escaping rules as `redis.conf`.
  """
  @spec directive(String.t(), [term()]) :: String.t()
  def directive(key, values) when is_binary(key) and is_list(values) do
    validate_directive_key!(key)
    Enum.join([key | Enum.map(values, &encode_token/1)], " ")
  end

  @doc """
  Generates redis.conf file content from the config.
  """
  @spec to_config_string(t()) :: String.t()
  def to_config_string(%__MODULE__{} = config) do
    []
    |> emit("port", config.port)
    |> emit("bind", bind_tokens(config.bind))
    |> emit_auth(config)
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

  defp emit(acc, key, values) when is_list(values) do
    [directive(key, values) | acc]
  end

  defp emit(acc, key, value), do: emit(acc, key, [value])

  defp emit_if(acc, _key, nil), do: acc
  defp emit_if(acc, key, value), do: emit(acc, key, value)

  defp emit_save(acc, :default), do: acc
  defp emit_save(acc, :disabled), do: emit(acc, "save", [""])

  defp emit_save(acc, policies) when is_list(policies) do
    Enum.reduce(policies, acc, fn {seconds, changes}, acc ->
      emit(acc, "save", [seconds, changes])
    end)
  end

  defp emit_auth(acc, %{username: nil, password: password}) do
    emit_if(acc, "requirepass", password)
  end

  defp emit_auth(acc, %{username: "default", password: password}) do
    emit(acc, "user", ["default", "on", ">#{password}", "~*", "&*", "+@all"])
  end

  defp emit_auth(acc, %{username: username, password: password}) do
    acc
    |> emit("user", ["default", "off"])
    |> emit("user", [username, "on", ">#{password}", "~*", "&*", "+@all"])
  end

  defp emit_tls(acc, %{tls_port: nil}), do: acc

  defp emit_tls(acc, config) do
    acc
    |> emit("tls-port", config.tls_port)
    |> emit_if("tls-cert-file", config.tls_cert_file)
    |> emit_if("tls-key-file", config.tls_key_file)
    |> emit_if("tls-ca-cert-file", config.tls_ca_cert_file)
    |> emit_if("tls-ca-cert-dir", config.tls_ca_cert_dir)
    |> emit_if("tls-auth-clients", config.tls_auth_clients)
    |> emit_if_true("tls-replication", config.tls_replication)
    |> emit_if_true("tls-cluster", config.tls_cluster)
  end

  defp emit_replication(acc, %{replicaof: nil}), do: acc

  defp emit_replication(acc, config) do
    {host, port} = config.replicaof

    acc
    |> emit("replicaof", [host, port])
    |> emit_if("masteruser", config.masteruser)
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
    Enum.reduce(modules, acc, &emit_module/2)
  end

  defp emit_module(module, acc) when is_binary(module), do: emit(acc, "loadmodule", [module])
  defp emit_module({path, args}, acc), do: emit(acc, "loadmodule", [path | args])

  defp emit_extra(acc, []), do: acc

  defp emit_extra(acc, extras) do
    Enum.reduce(extras, acc, fn
      {key, values}, acc when is_list(values) -> emit(acc, key, values)
      {key, value}, acc -> emit(acc, key, [value])
    end)
  end

  @reserved_directives MapSet.new(~w(
    port bind requirepass user loglevel logfile daemonize pidfile dir save
    appendonly appendfsync maxmemory maxmemory-policy tcp-backlog timeout
    tcp-keepalive unixsocket unixsocketperm tls-port tls-cert-file
    tls-key-file tls-ca-cert-file tls-ca-cert-dir tls-auth-clients
    tls-replication tls-cluster replicaof masteruser masterauth
    cluster-enabled cluster-config-file cluster-node-timeout
    cluster-announce-hostname cluster-announce-port cluster-announce-bus-port
    loadmodule
  ))

  defp validate!(config) do
    validate_enum!(:loglevel, config.loglevel, [:debug, :verbose, :notice, :warning])
    validate_enum!(:appendfsync, config.appendfsync, [:always, :everysec, :no])
    validate_boolean!(:daemonize, config.daemonize)
    validate_boolean!(:appendonly, config.appendonly)
    validate_boolean!(:cluster_enabled, config.cluster_enabled)
    validate_boolean!(:tls_insecure, config.tls_insecure)
    validate_boolean!(:tls_replication, config.tls_replication)
    validate_boolean!(:tls_cluster, config.tls_cluster)
    validate_listen_port!(:port, config.port)

    Enum.each(
      [
        tcp_backlog: config.tcp_backlog,
        timeout: config.timeout,
        tcp_keepalive: config.tcp_keepalive,
        cluster_node_timeout: config.cluster_node_timeout
      ],
      fn {field, value} -> validate_optional_non_negative_integer!(field, value) end
    )

    Enum.each(
      [
        tls_port: config.tls_port,
        cluster_announce_port: config.cluster_announce_port,
        cluster_announce_bus_port: config.cluster_announce_bus_port
      ],
      fn {field, value} -> validate_optional_port!(field, value) end
    )

    validate_bind!(config.bind)
    validate_control_host!(config)

    Enum.each(
      [
        username: config.username,
        password: config.password,
        logfile: config.logfile,
        pidfile: config.pidfile,
        dir: config.dir,
        maxmemory: config.maxmemory,
        maxmemory_policy: config.maxmemory_policy,
        unixsocket: config.unixsocket,
        unixsocketperm: config.unixsocketperm,
        tls_cert_file: config.tls_cert_file,
        tls_key_file: config.tls_key_file,
        tls_ca_cert_file: config.tls_ca_cert_file,
        tls_ca_cert_dir: config.tls_ca_cert_dir,
        tls_auth_clients: config.tls_auth_clients,
        tls_client_cert_file: config.tls_client_cert_file,
        tls_client_key_file: config.tls_client_key_file,
        tls_server_name: config.tls_server_name,
        masteruser: config.masteruser,
        masterauth: config.masterauth,
        cluster_config_file: config.cluster_config_file,
        cluster_announce_hostname: config.cluster_announce_hostname
      ],
      fn {field, value} -> validate_optional_binary!(field, value) end
    )

    validate_save!(config.save)
    validate_auth!(config)
    validate_tls!(config)
    validate_endpoint!(config)
    validate_unix_socket!(config.unixsocket)
    validate_replicaof!(config.replicaof)
    validate_replication_auth!(config)
    validate_modules!(config.loadmodule)
    validate_extra!(config.extra)
  end

  defp validate_enum!(field, value, allowed) do
    unless value in allowed do
      raise ArgumentError, ":#{field} must be one of #{inspect(allowed)}, got: #{inspect(value)}"
    end
  end

  defp validate_boolean!(_field, value) when is_boolean(value), do: :ok

  defp validate_boolean!(field, value) do
    raise ArgumentError, ":#{field} must be a boolean, got: #{inspect(value)}"
  end

  defp validate_port!(_field, value) when is_integer(value) and value in 1..65_535, do: :ok

  defp validate_port!(field, value) do
    raise ArgumentError, ":#{field} must be an integer from 1 to 65535, got: #{inspect(value)}"
  end

  defp validate_listen_port!(_field, value)
       when is_integer(value) and value in 0..65_535,
       do: :ok

  defp validate_listen_port!(field, value) do
    raise ArgumentError, ":#{field} must be an integer from 0 to 65535, got: #{inspect(value)}"
  end

  defp validate_optional_port!(_field, nil), do: :ok
  defp validate_optional_port!(field, value), do: validate_port!(field, value)

  defp validate_optional_non_negative_integer!(_field, nil), do: :ok

  defp validate_optional_non_negative_integer!(_field, value)
       when is_integer(value) and value >= 0,
       do: :ok

  defp validate_optional_non_negative_integer!(field, value) do
    raise ArgumentError, ":#{field} must be a non-negative integer, got: #{inspect(value)}"
  end

  defp validate_optional_binary!(_field, nil), do: :ok
  defp validate_optional_binary!(_field, value) when is_binary(value), do: :ok

  defp validate_optional_binary!(field, value) do
    raise ArgumentError, ":#{field} must be a string or nil, got: #{inspect(value)}"
  end

  defp validate_bind!(bind) do
    bind
    |> bind_tokens()
    |> validate_bind_tokens!()
  end

  defp validate_bind_tokens!([]), do: raise(ArgumentError, ":bind must contain an address")
  defp validate_bind_tokens!(addresses), do: Enum.each(addresses, &validate_bind_address!/1)

  defp validate_bind_address!(address) when is_binary(address) and address != "", do: :ok

  defp validate_bind_address!(_address) do
    raise ArgumentError, ":bind addresses must be non-empty strings"
  end

  defp validate_control_host!(config) do
    host = control_host(config)

    cond do
      config.port == 0 and is_nil(config.tls_port) ->
        :ok

      not is_binary(host) or host == "" or Regex.match?(~r/\s/u, host) ->
        raise ArgumentError, ":control_host must resolve to one non-empty address"

      is_nil(config.control_host) and host in ["*", "::*", "0.0.0.0", "::"] ->
        raise ArgumentError,
              ":control_host is required when the first bind address is a wildcard"

      true ->
        :ok
    end
  end

  defp validate_auth!(%{username: nil}), do: :ok

  defp validate_auth!(%{username: username, password: password})
       when is_binary(username) and username != "" and is_binary(password),
       do: :ok

  defp validate_auth!(config) do
    raise ArgumentError,
          ":username requires a non-empty username and string :password, got: " <>
            "#{inspect(config.username)} / #{inspect(config.password)}"
  end

  defp validate_tls!(%{tls_port: nil} = config) do
    tls_options = [
      config.tls_cert_file,
      config.tls_key_file,
      config.tls_ca_cert_file,
      config.tls_ca_cert_dir,
      config.tls_auth_clients,
      config.tls_client_cert_file,
      config.tls_client_key_file,
      config.tls_server_name
    ]

    if config.tls_insecure or config.tls_replication or config.tls_cluster or
         Enum.any?(tls_options, &is_binary/1) do
      raise ArgumentError, "TLS options require :tls_port"
    end
  end

  defp validate_tls!(config) do
    validate_tls_server_identity!(config)
    validate_tls_ca!(config)
    validate_tls_auth_clients!(config)
    validate_tls_client_identity!(config)
    _connection = connection(config)
    :ok
  end

  defp validate_tls_server_identity!(config) do
    required = [
      tls_cert_file: config.tls_cert_file,
      tls_key_file: config.tls_key_file
    ]

    case Enum.find(required, fn {_key, value} -> not (is_binary(value) and value != "") end) do
      {key, _value} -> raise ArgumentError, ":#{key} is required when :tls_port is set"
      nil -> :ok
    end
  end

  defp validate_tls_ca!(config) do
    if is_nil(config.tls_ca_cert_file) and is_nil(config.tls_ca_cert_dir) do
      raise ArgumentError,
            ":tls_ca_cert_file or :tls_ca_cert_dir is required when :tls_port is set"
    end

    if config.tls_ca_cert_file && config.tls_ca_cert_dir do
      raise ArgumentError, "set only one of :tls_ca_cert_file and :tls_ca_cert_dir"
    end
  end

  defp validate_tls_auth_clients!(config) do
    if config.tls_auth_clients not in [nil, "yes", "no", "optional"] do
      raise ArgumentError, ":tls_auth_clients must be \"yes\", \"no\", \"optional\", or nil"
    end
  end

  defp validate_tls_client_identity!(config) do
    if config.tls_auth_clients in [nil, "yes"] and
         (is_nil(config.tls_client_cert_file) or is_nil(config.tls_client_key_file)) do
      raise ArgumentError,
            "Redis requires TLS client certificates by default; set " <>
              ":tls_client_cert_file and :tls_client_key_file, or set " <>
              ":tls_auth_clients to \"no\" or \"optional\""
    end
  end

  defp validate_endpoint!(config) do
    unless config.port > 0 or not is_nil(config.tls_port) or not is_nil(config.unixsocket) do
      raise ArgumentError, "at least one of :port, :tls_port, or :unixsocket must be enabled"
    end
  end

  # sockaddr_un.sun_path is 104 bytes on macOS and 108 on Linux. Keep one byte
  # for the terminator and enforce the portable lower bound before Redis starts.
  defp validate_unix_socket!(nil), do: :ok

  defp validate_unix_socket!(path) when byte_size(path) <= 103, do: :ok

  defp validate_unix_socket!(path) do
    raise ArgumentError,
          ":unixsocket must be at most 103 bytes for portable Unix-socket support, got: " <>
            "#{byte_size(path)}"
  end

  defp validate_replication_auth!(%{replicaof: nil, masteruser: nil, masterauth: nil}), do: :ok

  defp validate_replication_auth!(%{replicaof: nil}) do
    raise ArgumentError, ":masteruser and :masterauth require :replicaof"
  end

  defp validate_replication_auth!(%{masteruser: nil}), do: :ok

  defp validate_replication_auth!(%{masterauth: masterauth}) when is_binary(masterauth), do: :ok

  defp validate_replication_auth!(_config) do
    raise ArgumentError, ":masteruser requires :masterauth"
  end

  defp validate_save!(:default), do: :ok
  defp validate_save!(:disabled), do: :ok

  defp validate_save!(policies) when is_list(policies) do
    Enum.each(policies, fn
      {seconds, changes}
      when is_integer(seconds) and seconds > 0 and is_integer(changes) and changes > 0 ->
        :ok

      policy ->
        raise ArgumentError,
              ":save entries must be {positive_seconds, positive_changes}, got: " <>
                inspect(policy)
    end)
  end

  defp validate_save!(value) do
    raise ArgumentError,
          ":save must be :default, :disabled, or a policy list, got: #{inspect(value)}"
  end

  defp validate_replicaof!(nil), do: :ok

  defp validate_replicaof!({host, port}) when is_binary(host) and host != "" do
    validate_port!(:replicaof_port, port)
  end

  defp validate_replicaof!(value) do
    raise ArgumentError, ":replicaof must be {host, port} or nil, got: #{inspect(value)}"
  end

  defp validate_modules!(modules) when is_list(modules) do
    Enum.each(modules, fn
      module when is_binary(module) and module != "" ->
        :ok

      {path, args} when is_binary(path) and path != "" and is_list(args) ->
        if Enum.all?(args, &is_binary/1) do
          :ok
        else
          invalid_module_spec!({path, args})
        end

      other ->
        invalid_module_spec!(other)
    end)
  end

  defp validate_modules!(other), do: invalid_module_spec!(other)

  defp invalid_module_spec!(value) do
    raise ArgumentError,
          ":loadmodule must be a list of paths or {path, [args]} tuples, got: " <>
            inspect(value)
  end

  defp validate_extra!(extras) when is_list(extras) do
    Enum.each(extras, fn
      {key, value} when is_binary(value) ->
        validate_extra_key!(key)

      {key, values} when is_list(values) ->
        validate_extra_key!(key)

        unless values != [] and Enum.all?(values, &is_binary/1) do
          invalid_extra_spec!({key, values})
        end

      extra ->
        invalid_extra_spec!(extra)
    end)
  end

  defp validate_extra!(value), do: invalid_extra_spec!(value)

  defp validate_extra_key!(key) when is_binary(key) do
    normalized = String.downcase(key)
    validate_directive_key!(normalized)

    if MapSet.member?(@reserved_directives, normalized) do
      raise ArgumentError,
            ":extra cannot override wrapper-owned directive #{inspect(normalized)}"
    end
  end

  defp validate_extra_key!(key), do: invalid_extra_spec!({key, []})

  defp invalid_extra_spec!(value) do
    raise ArgumentError,
          ":extra must be a list of {directive, value_or_argument_vector} tuples, got: " <>
            inspect(value)
  end

  defp validate_directive_key!(key) do
    unless Regex.match?(~r/\A[a-z][a-z0-9-]*\z/, String.downcase(key)) do
      raise ArgumentError, "invalid Redis directive name: #{inspect(key)}"
    end
  end

  defp encode_token(value) do
    value = to_string(value)

    if value != "" and Regex.match?(~r/\A[^\s"\\#]+\z/u, value) do
      value
    else
      quote_token(value)
    end
  end

  defp quote_token(value) do
    escaped =
      value
      |> String.replace("\\", "\\\\")
      |> String.replace("\"", "\\\"")
      |> String.replace("\n", "\\n")
      |> String.replace("\r", "\\r")
      |> String.replace("\t", "\\t")

    ~s("#{escaped}")
  end

  defp bind_tokens(bind) when is_binary(bind), do: String.split(bind)
  defp bind_tokens(bind) when is_list(bind), do: bind
  defp bind_tokens(_bind), do: []

  defp normalize_control_host("-" <> host), do: host
  defp normalize_control_host(host), do: host

  defp yn(true), do: "yes"
  defp yn(false), do: "no"

  defp emit_if_true(acc, _key, false), do: acc
  defp emit_if_true(acc, key, true), do: emit(acc, key, "yes")
end
