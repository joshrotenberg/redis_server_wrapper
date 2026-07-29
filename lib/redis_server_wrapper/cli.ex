defmodule RedisServerWrapper.Cli do
  @moduledoc """
  Wrapper around the `redis-cli` binary for running commands against Redis instances.
  """

  alias RedisServerWrapper.Connection

  @type t :: %__MODULE__{
          bin: String.t(),
          connection: Connection.t()
        }

  defstruct bin: "redis-cli", connection: %Connection{}

  @doc """
  Creates a new Cli struct.

      Cli.new(port: 6400, password: "secret")
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    {bin, opts} = Keyword.pop(opts, :bin, "redis-cli")

    connection =
      case Keyword.pop(opts, :connection) do
        {%Connection{} = connection, []} ->
          connection

        {nil, connection_opts} ->
          connection_opts
          |> normalize_legacy_options()
          |> Connection.new()

        {%Connection{}, remaining} ->
          raise ArgumentError,
                ":connection cannot be combined with connection options: #{inspect(remaining)}"
      end

    %__MODULE__{bin: bin, connection: connection}
  end

  @doc """
  Runs an arbitrary redis-cli command. Returns `{:ok, output}` or `{:error, reason}`.
  """
  @spec run(t(), [String.t()]) :: {:ok, String.t()} | {:error, String.t()}
  def run(%__MODULE__{} = cli, args) when is_list(args) do
    full_args = base_args(cli) ++ args

    case run_command(cli, full_args) do
      {output, 0} -> {:ok, String.trim(output)}
      {output, _code} -> {:error, String.trim(output)}
    end
  end

  @doc """
  Runs a command, raising on failure.
  """
  @spec run!(t(), [String.t()]) :: String.t()
  def run!(%__MODULE__{} = cli, args) do
    case run(cli, args) do
      {:ok, output} -> output
      {:error, reason} -> raise "redis-cli error: #{reason}"
    end
  end

  @doc """
  Sends PING, returns true if PONG received.
  """
  @spec ping(t()) :: boolean()
  def ping(%__MODULE__{} = cli) do
    case run(cli, ["PING"]) do
      {:ok, "PONG"} -> true
      _ -> false
    end
  end

  @doc """
  Sends SHUTDOWN NOSAVE. Best-effort, ignores errors.
  """
  @spec shutdown(t()) :: :ok
  def shutdown(%__MODULE__{} = cli) do
    # redis-cli tolerates the server closing the connection as part of shutdown.
    # Wait for the attempt to finish so a delayed command cannot hit a later
    # process that reuses the same endpoint.
    _result = run(cli, ["SHUTDOWN", "NOSAVE"])
    :ok
  end

  @doc """
  Polls with PING until the server responds or timeout (ms) is reached.

  Returns `{:error, {:unexpected_reply, output}}` if the peer accepts the
  connection but responds with something other than `PONG` (or a transient
  `LOADING`/`BUSY` reply). This catches the common case of another service
  holding the port (e.g. macOS AirPlay on 7000) or a different redis-server
  with a mismatched password already bound to the port.
  """
  @spec wait_for_ready(t(), non_neg_integer()) ::
          :ok | {:error, :timeout} | {:error, {:unexpected_reply, String.t()}}
  def wait_for_ready(%__MODULE__{} = cli, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_ready(cli, deadline)
  end

  defp do_wait_for_ready(cli, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      case ping_status(cli) do
        :ready ->
          :ok

        :not_ready ->
          Process.sleep(250)
          do_wait_for_ready(cli, deadline)

        {:unexpected, output} ->
          {:error, {:unexpected_reply, output}}
      end
    end
  end

  # Classify a PING attempt:
  #   :ready                - got PONG
  #   :not_ready            - connection refused, or transient LOADING/BUSY reply
  #   {:unexpected, output} - accepted the connection but replied with
  #                           something that isn't PONG or a known transient;
  #                           almost always means the port is held by a
  #                           different service (or a redis with different auth)
  defp ping_status(%__MODULE__{} = cli) do
    case run(cli, ["PING"]) do
      {:ok, "PONG"} -> :ready
      {:ok, output} -> classify_ok_reply(output)
      {:error, _} -> :not_ready
    end
  end

  defp classify_ok_reply(output) do
    cond do
      output =~ ~r/\bLOADING\b/ -> :not_ready
      output =~ ~r/\bBUSY\b/ -> :not_ready
      true -> {:unexpected, output}
    end
  end

  @doc """
  Runs `redis-cli --cluster create` to form a cluster from the given node addresses.
  """
  @spec cluster_create(t(), [String.t()], non_neg_integer()) ::
          {:ok, String.t()} | {:error, String.t()}
  def cluster_create(%__MODULE__{} = cli, node_addrs, replicas_per_master \\ 0) do
    args =
      Connection.cli_args(cli.connection, include_endpoint: false) ++
        ["--cluster", "create"] ++
        node_addrs ++
        ["--cluster-replicas", to_string(replicas_per_master), "--cluster-yes"]

    case run_command(cli, args) do
      {output, 0} -> {:ok, String.trim(output)}
      {output, _code} -> {:error, String.trim(output)}
    end
  end

  @doc """
  Runs `CLUSTER INFO` and returns it as a parsed map.
  """
  @spec cluster_info(t()) :: {:ok, map()} | {:error, String.t()}
  def cluster_info(%__MODULE__{} = cli) do
    case run(cli, ["CLUSTER", "INFO"]) do
      {:ok, output} -> {:ok, parse_info(output)}
      error -> error
    end
  end

  @doc """
  Runs `SENTINEL MASTER <name>` and returns parsed key-value map.
  """
  @spec sentinel_master(t(), String.t()) :: {:ok, map()} | {:error, String.t()}
  def sentinel_master(%__MODULE__{} = cli, master_name) do
    case run(cli, ["SENTINEL", "MASTER", master_name]) do
      {:ok, output} -> {:ok, parse_flat_kv(output)}
      error -> error
    end
  end

  # Build base connection arguments for redis-cli
  defp base_args(%__MODULE__{} = cli) do
    Connection.cli_args(cli.connection)
  end

  defp run_command(cli, args) do
    System.cmd(
      cli.bin,
      args,
      stderr_to_stdout: true,
      env: Connection.cli_env(cli.connection)
    )
  end

  defp normalize_legacy_options(opts) do
    tls = Keyword.get(opts, :tls, false)
    socket = Keyword.get(opts, :socket)

    transport =
      cond do
        socket -> :unix
        tls -> :tls
        true -> Keyword.get(opts, :transport, :tcp)
      end

    opts
    |> Keyword.delete(:tls)
    |> Keyword.put(:transport, transport)
  end

  # Parse "key:value\r\n" format (CLUSTER INFO, INFO)
  defp parse_info(output) do
    output
    |> String.split(~r/\r?\n/, trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Map.new(fn line ->
      case String.split(line, ":", parts: 2) do
        [k, v] -> {String.trim(k), String.trim(v)}
        _ -> {line, ""}
      end
    end)
  end

  # Parse flat key-value output (alternating key\nvalue\n lines from SENTINEL MASTER)
  defp parse_flat_kv(output) do
    output
    |> String.split(~r/\r?\n/, trim: true)
    |> Enum.chunk_every(2)
    |> Map.new(fn
      [k, v] -> {String.trim(k), String.trim(v)}
      [k] -> {String.trim(k), ""}
    end)
  end
end
