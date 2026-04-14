defmodule RedisServerWrapper do
  @moduledoc """
  Elixir wrapper for `redis-server` and `redis-cli` with GenServer-managed process lifecycles.

  Manage Redis server processes for testing, development, and CI without Docker --
  just `redis-server` and `redis-cli` on PATH.

  ## Quick Start

      # Single server
      {:ok, server} = RedisServerWrapper.start_server(port: 6400, password: "secret")
      RedisServerWrapper.Server.run(server, ["SET", "key", "value"])
      RedisServerWrapper.Server.stop(server)

      # Cluster
      {:ok, cluster} = RedisServerWrapper.start_cluster(masters: 3, base_port: 7100)
      RedisServerWrapper.Cluster.healthy?(cluster)
      RedisServerWrapper.Cluster.stop(cluster)

      # Sentinel
      {:ok, sentinel} = RedisServerWrapper.start_sentinel(master_port: 6390, replicas: 2, sentinels: 3)
      RedisServerWrapper.Sentinel.healthy?(sentinel)
      RedisServerWrapper.Sentinel.stop(sentinel)

  ## Features

    * **Single server** - start/stop with options, auto-cleanup on GenServer terminate
    * **Cluster** - spin up N-master clusters with optional replicas
    * **Sentinel** - full sentinel topology (master + replicas + sentinels)
    * **Custom binaries** - point to any `redis-server`/`redis-cli` path
    * **Arbitrary config** - pass any Redis directive via `:extra` option
  """

  alias RedisServerWrapper.{Cluster, Sentinel, Server}

  @doc """
  Starts a single redis-server instance. See `RedisServerWrapper.Server` for options.
  """
  @spec start_server(keyword()) :: GenServer.on_start()
  defdelegate start_server(opts \\ []), to: Server, as: :start_link

  @doc """
  Starts a Redis Cluster. See `RedisServerWrapper.Cluster` for options.
  """
  @spec start_cluster(keyword()) :: GenServer.on_start()
  defdelegate start_cluster(opts \\ []), to: Cluster, as: :start_link

  @doc """
  Starts a Redis Sentinel topology. See `RedisServerWrapper.Sentinel` for options.
  """
  @spec start_sentinel(keyword()) :: GenServer.on_start()
  defdelegate start_sentinel(opts \\ []), to: Sentinel, as: :start_link

  @doc """
  Checks if `redis-server` is available on PATH (or at the given path).
  """
  @spec available?(String.t()) :: boolean()
  def available?(bin \\ "redis-server") do
    System.find_executable(bin) != nil
  end

  @doc """
  Returns the version of redis-server on PATH, or `{:error, reason}`.
  """
  @spec version(String.t()) :: {:ok, String.t()} | {:error, term()}
  def version(bin \\ "redis-server") do
    with path when not is_nil(path) <- System.find_executable(bin),
         {output, 0} <- System.cmd(path, ["--version"], stderr_to_stdout: true) do
      case Regex.run(~r/v=(\S+)/, output) do
        [_, version] -> {:ok, version}
        _ -> {:ok, String.trim(output)}
      end
    else
      nil -> {:error, {:binary_not_found, bin}}
      {output, _} -> {:error, output}
    end
  end
end
