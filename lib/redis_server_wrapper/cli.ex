defmodule RedisServerWrapper.Cli do
  @moduledoc """
  Wrapper around the `redis-cli` binary for running commands against Redis instances.
  """

  @type t :: %__MODULE__{
          bin: String.t(),
          host: String.t(),
          port: non_neg_integer(),
          password: String.t() | nil,
          tls: boolean()
        }

  defstruct bin: "redis-cli",
            host: "127.0.0.1",
            port: 6379,
            password: nil,
            tls: false

  @doc """
  Creates a new Cli struct.

      Cli.new(port: 6400, password: "secret")
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    struct!(__MODULE__, opts)
  end

  @doc """
  Runs an arbitrary redis-cli command. Returns `{:ok, output}` or `{:error, reason}`.
  """
  @spec run(t(), [String.t()]) :: {:ok, String.t()} | {:error, String.t()}
  def run(%__MODULE__{} = cli, args) when is_list(args) do
    full_args = base_args(cli) ++ args

    case System.cmd(cli.bin, full_args, stderr_to_stdout: true) do
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
    # Fire and forget - the connection will be closed by the server
    spawn(fn -> run(cli, ["SHUTDOWN", "NOSAVE"]) end)
    :ok
  end

  @doc """
  Polls with PING until the server responds or timeout (ms) is reached.
  """
  @spec wait_for_ready(t(), non_neg_integer()) :: :ok | {:error, :timeout}
  def wait_for_ready(%__MODULE__{} = cli, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_ready(cli, deadline)
  end

  defp do_wait_for_ready(cli, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      if ping(cli) do
        :ok
      else
        Process.sleep(250)
        do_wait_for_ready(cli, deadline)
      end
    end
  end

  @doc """
  Runs `redis-cli --cluster create` to form a cluster from the given node addresses.
  """
  @spec cluster_create(t(), [String.t()], non_neg_integer()) ::
          {:ok, String.t()} | {:error, String.t()}
  def cluster_create(%__MODULE__{} = cli, node_addrs, replicas_per_master \\ 0) do
    args =
      ["--cluster", "create"] ++
        node_addrs ++
        ["--cluster-replicas", to_string(replicas_per_master), "--cluster-yes"] ++
        auth_args(cli)

    case System.cmd(cli.bin, args, stderr_to_stdout: true) do
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
    ["-h", cli.host, "-p", to_string(cli.port)]
    |> maybe_append(cli.password, fn pw -> ["-a", pw, "--no-auth-warning"] end)
    |> maybe_append(cli.tls, fn _ -> ["--tls"] end)
  end

  defp auth_args(%__MODULE__{password: nil}), do: []
  defp auth_args(%__MODULE__{password: pw}), do: ["-a", pw, "--no-auth-warning"]

  defp maybe_append(args, nil, _fun), do: args
  defp maybe_append(args, false, _fun), do: args
  defp maybe_append(args, value, fun), do: args ++ fun.(value)

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
