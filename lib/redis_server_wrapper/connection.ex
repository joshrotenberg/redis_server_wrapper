defmodule RedisServerWrapper.Connection do
  @moduledoc """
  A Redis client connection shared by lifecycle and topology APIs.

  The connection keeps the endpoint, authentication, and TLS client settings
  together so readiness, commands, health checks, shutdown, and persisted
  Manager instances all use the same transport.
  """

  @type transport :: :tcp | :tls | :unix

  @type t :: %__MODULE__{
          transport: transport(),
          host: String.t() | nil,
          port: :inet.port_number() | nil,
          socket: String.t() | nil,
          username: String.t() | nil,
          password: String.t() | nil,
          tls_ca_cert_file: String.t() | nil,
          tls_ca_cert_dir: String.t() | nil,
          tls_client_cert_file: String.t() | nil,
          tls_client_key_file: String.t() | nil,
          tls_server_name: String.t() | nil,
          tls_insecure: boolean()
        }

  @derive {JSON.Encoder,
           only: [
             :transport,
             :host,
             :port,
             :socket,
             :username,
             :password,
             :tls_ca_cert_file,
             :tls_ca_cert_dir,
             :tls_client_cert_file,
             :tls_client_key_file,
             :tls_server_name,
             :tls_insecure
           ]}
  defstruct transport: :tcp,
            host: "127.0.0.1",
            port: 6379,
            socket: nil,
            username: nil,
            password: nil,
            tls_ca_cert_file: nil,
            tls_ca_cert_dir: nil,
            tls_client_cert_file: nil,
            tls_client_key_file: nil,
            tls_server_name: nil,
            tls_insecure: false

  @doc "Creates and validates a connection."
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    connection = struct!(__MODULE__, opts)
    validate!(connection)
    connection
  end

  @doc false
  @spec from_map(map()) :: t()
  def from_map(map) when is_map(map) do
    defaults = Map.from_struct(%__MODULE__{})
    fields = Map.keys(defaults)

    opts =
      Enum.map(fields, fn field ->
        value =
          Map.get(map, field, Map.get(map, Atom.to_string(field), Map.fetch!(defaults, field)))

        {field, normalize_loaded_value(field, value)}
      end)

    new(opts)
  end

  @doc "Returns a copy of a TCP or TLS connection using another port."
  @spec with_port(t(), :inet.port_number()) :: t()
  def with_port(%__MODULE__{transport: transport} = connection, port)
      when transport in [:tcp, :tls] do
    connection
    |> Map.from_struct()
    |> Map.put(:port, port)
    |> Map.to_list()
    |> new()
  end

  @doc "Returns the listener used for ownership and availability checks."
  @spec listener(t()) :: {:tcp, String.t(), :inet.port_number()} | {:unix, String.t()}
  def listener(%__MODULE__{transport: :unix, socket: socket}), do: {:unix, socket}

  def listener(%__MODULE__{host: host, port: port}) do
    {:tcp, host, port}
  end

  @doc "Returns the redis-cli connection argument vector."
  @spec cli_args(t(), keyword()) :: [String.t()]
  def cli_args(%__MODULE__{} = connection, opts \\ []) do
    include_endpoint = Keyword.get(opts, :include_endpoint, true)

    []
    |> append_endpoint(connection, include_endpoint)
    |> append_username(connection.username)
    |> append_tls(connection)
  end

  @doc "Returns environment variables used to pass secrets to redis-cli."
  @spec cli_env(t()) :: [{String.t(), String.t()}]
  def cli_env(%__MODULE__{password: nil}), do: []
  def cli_env(%__MODULE__{password: password}), do: [{"REDISCLI_AUTH", password}]

  @doc "Returns a Redis URL for display or client handoff."
  @spec url(t()) :: String.t()
  def url(%__MODULE__{transport: :unix, socket: socket}) do
    "unix://#{URI.encode(socket)}"
  end

  def url(%__MODULE__{} = connection) do
    scheme = if connection.transport == :tls, do: "rediss", else: "redis"
    credentials = encoded_credentials(connection)
    "#{scheme}://#{credentials}#{url_host(connection.host)}:#{connection.port}"
  end

  @doc "Returns a stable, filesystem-safe endpoint identifier."
  @spec id(t()) :: String.t()
  def id(%__MODULE__{transport: :unix, socket: socket}) do
    hash = :crypto.hash(:sha256, socket) |> Base.encode16(case: :lower) |> binary_part(0, 12)
    "unix-#{hash}"
  end

  def id(%__MODULE__{port: port}), do: Integer.to_string(port)

  @doc "Returns a concise endpoint label suitable for errors and logs."
  @spec label(t()) :: String.t()
  def label(%__MODULE__{transport: :unix, socket: socket}), do: socket
  def label(%__MODULE__{host: host, port: port}), do: "#{host}:#{port}"

  defp append_endpoint(args, _connection, false), do: args

  defp append_endpoint(args, %__MODULE__{transport: :unix, socket: socket}, true) do
    args ++ ["-s", socket]
  end

  defp append_endpoint(args, %__MODULE__{host: host, port: port}, true) do
    args ++ ["-h", host, "-p", to_string(port)]
  end

  defp append_username(args, nil), do: args
  defp append_username(args, username), do: args ++ ["--user", username]

  defp append_tls(args, %__MODULE__{transport: transport}) when transport != :tls, do: args

  defp append_tls(args, connection) do
    args
    |> Kernel.++(["--tls"])
    |> maybe_append(connection.tls_ca_cert_file, "--cacert")
    |> maybe_append(connection.tls_ca_cert_dir, "--cacertdir")
    |> maybe_append(connection.tls_client_cert_file, "--cert")
    |> maybe_append(connection.tls_client_key_file, "--key")
    |> maybe_append(connection.tls_server_name, "--sni")
    |> maybe_append_flag(connection.tls_insecure, "--insecure")
  end

  defp maybe_append(args, nil, _flag), do: args
  defp maybe_append(args, value, flag), do: args ++ [flag, value]

  defp maybe_append_flag(args, false, _flag), do: args
  defp maybe_append_flag(args, true, flag), do: args ++ [flag]

  defp encoded_credentials(%__MODULE__{username: nil, password: nil}), do: ""

  defp encoded_credentials(%__MODULE__{username: nil, password: password}) do
    ":#{encode_uri(password)}@"
  end

  defp encoded_credentials(%__MODULE__{username: username, password: password}) do
    "#{encode_uri(username)}:#{encode_uri(password)}@"
  end

  defp encode_uri(value), do: URI.encode(value, &URI.char_unreserved?/1)

  defp url_host(host) do
    if String.contains?(host, ":") and not String.starts_with?(host, "[") do
      "[#{host}]"
    else
      host
    end
  end

  defp validate!(%__MODULE__{} = connection) do
    validate_transport!(connection)
    validate_auth!(connection)
    validate_tls_options!(connection)
    validate_tls_client_pair!(connection)
    connection
  end

  defp validate_transport!(%__MODULE__{transport: :unix, socket: socket}) do
    unless is_binary(socket) and socket != "" do
      raise ArgumentError, ":socket must be a non-empty path for a Unix connection"
    end
  end

  defp validate_transport!(%__MODULE__{transport: transport, host: host, port: port})
       when transport in [:tcp, :tls] do
    unless is_binary(host) and host != "" do
      raise ArgumentError, ":host must be a non-empty string"
    end

    unless is_integer(port) and port in 1..65_535 do
      raise ArgumentError, ":port must be an integer from 1 to 65535"
    end
  end

  defp validate_transport!(%__MODULE__{transport: transport}) do
    raise ArgumentError, ":transport must be :tcp, :tls, or :unix, got: #{inspect(transport)}"
  end

  defp validate_auth!(%__MODULE__{username: nil, password: password})
       when is_nil(password) or is_binary(password),
       do: :ok

  defp validate_auth!(%__MODULE__{username: username, password: password})
       when is_binary(username) and username != "" and is_binary(password),
       do: :ok

  defp validate_auth!(%__MODULE__{username: username, password: password}) do
    raise ArgumentError,
          ":username requires a non-empty username and string password, got: " <>
            "#{inspect(username)} / #{inspect(password)}"
  end

  defp validate_tls_options!(connection) do
    Enum.each(
      [
        tls_ca_cert_file: connection.tls_ca_cert_file,
        tls_ca_cert_dir: connection.tls_ca_cert_dir,
        tls_client_cert_file: connection.tls_client_cert_file,
        tls_client_key_file: connection.tls_client_key_file,
        tls_server_name: connection.tls_server_name
      ],
      fn
        {_field, nil} ->
          :ok

        {_field, value} when is_binary(value) and value != "" ->
          :ok

        {field, value} ->
          raise ArgumentError,
                ":#{field} must be a non-empty string or nil, got: #{inspect(value)}"
      end
    )

    unless is_boolean(connection.tls_insecure) do
      raise ArgumentError, ":tls_insecure must be a boolean"
    end

    tls_options? =
      connection.tls_insecure or
        Enum.any?(
          [
            connection.tls_ca_cert_file,
            connection.tls_ca_cert_dir,
            connection.tls_client_cert_file,
            connection.tls_client_key_file,
            connection.tls_server_name
          ],
          &is_binary/1
        )

    if connection.transport != :tls and tls_options? do
      raise ArgumentError, "TLS client options require transport: :tls"
    end
  end

  defp validate_tls_client_pair!(%__MODULE__{
         tls_client_cert_file: nil,
         tls_client_key_file: nil
       }),
       do: :ok

  defp validate_tls_client_pair!(%__MODULE__{
         tls_client_cert_file: cert,
         tls_client_key_file: key
       })
       when is_binary(cert) and cert != "" and is_binary(key) and key != "",
       do: :ok

  defp validate_tls_client_pair!(_connection) do
    raise ArgumentError, ":tls_client_cert_file and :tls_client_key_file must be set together"
  end

  defp normalize_loaded_value(:transport, "tcp"), do: :tcp
  defp normalize_loaded_value(:transport, "tls"), do: :tls
  defp normalize_loaded_value(:transport, "unix"), do: :unix
  defp normalize_loaded_value(_field, value), do: value
end
