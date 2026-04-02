defmodule RedisServerWrapper.Chaos do
  @moduledoc """
  Fault injection primitives for testing Redis resilience.

  Provides tools to kill, pause, partition, and degrade Redis nodes
  managed by `RedisServerWrapper.Server` and `RedisServerWrapper.Cluster`.

  ## Node-level operations

      # Kill a node with SIGKILL
      Chaos.kill_node(server)

      # Freeze a node (SIGSTOP), then resume later with the returned OS pid
      {:ok, os_pid} = Chaos.freeze_node(server)
      Chaos.resume_node(os_pid)

      # Freeze for a specific duration (auto-resumes)
      Chaos.pause_node(server, 5_000)

      # Pause all client connections for a duration
      Chaos.slow_down(server, 2_000)

  ## Cluster-level operations

      # Kill the master owning a key's slot
      {:ok, killed_pid} = Chaos.kill_master(cluster, "mykey")

      # Kill the master owning slot 5000
      {:ok, killed_pid} = Chaos.kill_master(cluster, 5000)

      # Simulate a network partition (returns frozen OS pids for recovery)
      nodes = Cluster.nodes(cluster)
      {active, frozen} = Enum.split(nodes, 2)
      {:ok, frozen_os_pids} = Chaos.partition(cluster, [active, frozen])

      # Undo all chaos (SIGCONT everything)
      Chaos.recover(frozen_os_pids)
  """

  alias RedisServerWrapper.{Cluster, Server}

  # -------------------------------------------------------------------
  # Node-level operations
  # -------------------------------------------------------------------

  @doc """
  Sends SIGKILL to a server's redis-server OS process.

  The process is killed immediately and cannot be recovered.
  The GenServer will still be alive but the underlying redis-server will be dead.
  """
  @spec kill_node(GenServer.server()) :: :ok
  def kill_node(server) do
    %{pid: os_pid} = Server.info(server)
    System.cmd("kill", ["-9", to_string(os_pid)], stderr_to_stdout: true)
    :ok
  end

  @doc """
  Sends SIGSTOP to freeze a node for `duration_ms`, then automatically sends SIGCONT.

  The node will be completely unresponsive for the duration, simulating a process freeze
  or a very long GC pause. Returns `{:ok, os_pid}` with the OS process ID.
  """
  @spec pause_node(GenServer.server(), non_neg_integer()) :: {:ok, non_neg_integer()}
  def pause_node(server, duration_ms) do
    %{pid: os_pid} = Server.info(server)
    System.cmd("kill", ["-STOP", to_string(os_pid)], stderr_to_stdout: true)

    spawn(fn ->
      Process.sleep(duration_ms)
      System.cmd("kill", ["-CONT", to_string(os_pid)], stderr_to_stdout: true)
    end)

    {:ok, os_pid}
  end

  @doc """
  Sends SIGSTOP to freeze a node indefinitely.

  Returns `{:ok, os_pid}` with the OS process ID. Pass the OS pid to
  `resume_node/1` to unfreeze. The OS pid is needed because the Server GenServer
  will be blocked while the redis-server process is frozen.
  """
  @spec freeze_node(GenServer.server()) :: {:ok, non_neg_integer()}
  def freeze_node(server) do
    %{pid: os_pid} = Server.info(server)
    System.cmd("kill", ["-STOP", to_string(os_pid)], stderr_to_stdout: true)
    {:ok, os_pid}
  end

  @doc """
  Sends SIGCONT to resume a frozen node.

  Accepts an OS pid (integer) as returned by `freeze_node/1` or `partition/2`.
  """
  @spec resume_node(non_neg_integer()) :: :ok
  def resume_node(os_pid) when is_integer(os_pid) do
    System.cmd("kill", ["-CONT", to_string(os_pid)], stderr_to_stdout: true)
    :ok
  end

  @doc """
  Pauses all client connections for `duration_ms` using Redis CLIENT PAUSE.

  Unlike `freeze_node/1`, the server process stays alive and responsive to health
  checks via the admin interface, but all client commands are delayed.
  """
  @spec slow_down(GenServer.server(), non_neg_integer()) ::
          {:ok, String.t()} | {:error, String.t()}
  def slow_down(server, duration_ms) do
    Server.run(server, ["CLIENT", "PAUSE", to_string(duration_ms)])
  end

  @doc """
  Wipes all data from a node with FLUSHALL.
  """
  @spec flushall(GenServer.server()) :: {:ok, String.t()} | {:error, String.t()}
  def flushall(server) do
    Server.run(server, ["FLUSHALL"])
  end

  @doc """
  Fills a node with `key_count` dummy keys (1 KB each) under the `chaos:fill:*` prefix.

  Useful for testing memory pressure, eviction policies, and OOM behavior.
  """
  @spec fill_memory(GenServer.server(), non_neg_integer()) :: :ok
  def fill_memory(server, key_count \\ 10_000) do
    value = String.duplicate("x", 1024)

    Enum.each(1..key_count, fn i ->
      Server.run(server, ["SET", "chaos:fill:#{i}", value])
    end)

    :ok
  end

  @doc """
  Triggers a background save (BGSAVE) on the node.

  Can be used to test behavior during RDB persistence.
  """
  @spec trigger_save(GenServer.server()) :: {:ok, String.t()} | {:error, String.t()}
  def trigger_save(server) do
    Server.run(server, ["BGSAVE"])
  end

  # -------------------------------------------------------------------
  # Cluster-level operations
  # -------------------------------------------------------------------

  @doc """
  Finds and kills (SIGKILL) the master node owning a given slot or key.

  Accepts either a slot number (integer) or a key (string). When given a key,
  the slot is computed via CLUSTER KEYSLOT.

  Returns `{:ok, server_pid}` with the GenServer pid of the killed node,
  or `{:error, reason}` if the master could not be found.

  ## Examples

      {:ok, killed} = Chaos.kill_master(cluster, "user:123")
      {:ok, killed} = Chaos.kill_master(cluster, 5000)
  """
  @spec kill_master(GenServer.server(), String.t() | non_neg_integer()) ::
          {:ok, pid()} | {:error, term()}
  def kill_master(cluster, key) when is_binary(key) do
    case Cluster.run(cluster, ["CLUSTER", "KEYSLOT", key]) do
      {:ok, slot_str} -> kill_master(cluster, String.to_integer(slot_str))
      {:error, reason} -> {:error, {:keyslot_failed, reason}}
    end
  end

  def kill_master(cluster, slot) when is_integer(slot) do
    case find_master_for_slot(cluster, slot) do
      {:ok, server_pid} ->
        kill_node(server_pid)
        {:ok, server_pid}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Simulates a network partition by freezing (SIGSTOP) all nodes not in the first group.

  `groups` is a list of lists of Server GenServer pids (as returned by `Cluster.nodes/1`).
  The first group remains active; all other groups are frozen.

  Returns `{:ok, os_pids}` with the OS pids of frozen nodes. Pass these to
  `recover/1` to resume them.

  ## Example

      nodes = Cluster.nodes(cluster)
      {active, frozen} = Enum.split(nodes, 2)
      {:ok, frozen_os_pids} = Chaos.partition(cluster, [active, frozen])

      # Later...
      Chaos.recover(frozen_os_pids)
  """
  @spec partition(GenServer.server(), [[pid()]]) :: {:ok, [non_neg_integer()]}
  def partition(_cluster, [_active | frozen_groups]) do
    os_pids =
      frozen_groups
      |> List.flatten()
      |> Enum.map(fn server ->
        {:ok, os_pid} = freeze_node(server)
        os_pid
      end)

    {:ok, os_pids}
  end

  @doc """
  Kills a random node in the cluster. Returns `{:ok, server_pid}` of the killed node.
  """
  @spec random_kill(GenServer.server()) :: {:ok, pid()}
  def random_kill(cluster) do
    node = cluster |> Cluster.nodes() |> Enum.random()
    kill_node(node)
    {:ok, node}
  end

  @doc """
  Forces a replica to take over from its master via CLUSTER FAILOVER.

  The `replica` argument should be a Server GenServer pid for a replica node.

  ## Options

    * `:force` - if `true`, sends `CLUSTER FAILOVER FORCE` directly (default: `false`)

  When `force` is `false`, tries `CLUSTER FAILOVER` first. If the master is down
  (Redis returns a "Master is down or failed" error), automatically retries with
  `CLUSTER FAILOVER FORCE`.
  """
  @spec trigger_failover(GenServer.server(), keyword()) ::
          {:ok, String.t()} | {:error, String.t()}
  def trigger_failover(replica, opts \\ []) do
    if Keyword.get(opts, :force, false) do
      Server.run(replica, ["CLUSTER", "FAILOVER", "FORCE"])
    else
      failover_with_auto_force(replica)
    end
  end

  defp failover_with_auto_force(replica) do
    case Server.run(replica, ["CLUSTER", "FAILOVER"]) do
      {:ok, "ERR" <> _ = msg} ->
        if String.contains?(msg, "Master is down or failed") do
          Server.run(replica, ["CLUSTER", "FAILOVER", "FORCE"])
        else
          {:error, msg}
        end

      other ->
        other
    end
  end

  @doc """
  Recovers frozen nodes by sending SIGCONT to each OS pid.

  Accepts a list of OS pids as returned by `freeze_node/1` or `partition/2`.
  Nodes that were killed (not just frozen) are silently skipped.
  """
  @spec recover([non_neg_integer()]) :: :ok
  def recover(os_pids) when is_list(os_pids) do
    Enum.each(os_pids, &resume_node/1)
    :ok
  end

  # -------------------------------------------------------------------
  # Internal helpers
  # -------------------------------------------------------------------

  defp find_master_for_slot(cluster, target_slot) do
    node_pids = Cluster.nodes(cluster)

    # Get CLUSTER NODES output from any responsive node
    cluster_nodes_output =
      Enum.find_value(node_pids, fn pid ->
        case Server.run(pid, ["CLUSTER", "NODES"]) do
          {:ok, output} -> output
          _ -> nil
        end
      end)

    with output when not is_nil(output) <- cluster_nodes_output,
         {:ok, {host, port}} <- find_master_addr_for_slot(output, target_slot) do
      match_master_pid(node_pids, host, port)
    else
      nil -> {:error, :no_responsive_nodes}
      {:error, _} = error -> error
    end
  end

  defp match_master_pid(node_pids, host, port) do
    match =
      Enum.find(node_pids, fn pid ->
        info = Server.info(pid)
        info.host == host && info.port == port
      end)

    if match, do: {:ok, match}, else: {:error, :master_pid_not_found}
  end

  defp find_master_addr_for_slot(cluster_nodes_output, target_slot) do
    master =
      cluster_nodes_output
      |> String.split("\n", trim: true)
      |> Enum.find(fn line ->
        parts = String.split(line)
        # parts: [id, addr, flags, master_id, ping, pong, epoch, state, ...slots]
        length(parts) >= 9 &&
          String.contains?(Enum.at(parts, 2), "master") &&
          slot_ranges_contain?(Enum.drop(parts, 8), target_slot)
      end)

    case master do
      nil ->
        {:error, {:slot_not_found, target_slot}}

      line ->
        # Address format: "127.0.0.1:7000@17000" or "127.0.0.1:7000@17000,hostname"
        addr_part = line |> String.split() |> Enum.at(1)
        [host_port | _] = String.split(addr_part, "@")
        [host, port_str] = String.split(host_port, ":")
        {:ok, {host, String.to_integer(port_str)}}
    end
  end

  defp slot_ranges_contain?(slot_parts, target_slot) do
    Enum.any?(slot_parts, &slot_range_match?(&1, target_slot))
  end

  defp slot_range_match?(part, target_slot) do
    case String.split(part, "-") do
      [start_str, end_str] ->
        target_slot >= String.to_integer(start_str) &&
          target_slot <= String.to_integer(end_str)

      [single] ->
        match?({^target_slot, ""}, Integer.parse(single))
    end
  end
end
