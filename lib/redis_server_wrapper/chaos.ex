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

  alias RedisServerWrapper.{Cluster, OSProcess, Server}

  require Logger

  # -------------------------------------------------------------------
  # Node-level operations
  # -------------------------------------------------------------------

  @doc """
  Sends SIGKILL to a server's redis-server OS process.

  The process is killed immediately and cannot be recovered. Managed `Server`
  processes exit after observing the child death.
  """
  @spec kill_node(GenServer.server()) :: :ok | {:error, term()}
  def kill_node(server) do
    with {:ok, os_pid} <- server_os_pid(server),
         do: OSProcess.signal(os_pid, :kill)
  end

  @doc """
  Sends SIGSTOP to freeze a node for `duration_ms`, then automatically sends SIGCONT.

  The node will be completely unresponsive for the duration, simulating a process freeze
  or a very long GC pause. Returns `{:ok, os_pid}` with the OS process ID.
  """
  @spec pause_node(GenServer.server(), non_neg_integer()) ::
          {:ok, pos_integer()} | {:error, term()}
  def pause_node(server, duration_ms)
      when is_integer(duration_ms) and duration_ms >= 0 do
    with {:ok, os_pid} <- server_os_pid(server),
         :ok <- OSProcess.signal(os_pid, :stop) do
      spawn(fn -> resume_after(os_pid, duration_ms) end)
      {:ok, os_pid}
    end
  end

  def pause_node(_server, duration_ms),
    do: {:error, {:invalid_duration, duration_ms}}

  @doc """
  Sends SIGSTOP to freeze a node indefinitely.

  Returns `{:ok, os_pid}` with the OS process ID. Pass the OS pid to
  `resume_node/1` to unfreeze. The OS pid is needed because the Server GenServer
  will be blocked while the redis-server process is frozen.
  """
  @spec freeze_node(GenServer.server()) :: {:ok, pos_integer()} | {:error, term()}
  def freeze_node(server) do
    with {:ok, os_pid} <- server_os_pid(server),
         :ok <- OSProcess.signal(os_pid, :stop) do
      {:ok, os_pid}
    end
  end

  @doc """
  Sends SIGCONT to resume a frozen node.

  Accepts an OS pid (integer) as returned by `freeze_node/1` or `partition/2`.
  """
  @spec resume_node(pos_integer()) :: :ok | {:error, term()}
  def resume_node(os_pid) when is_integer(os_pid) and os_pid > 0,
    do: OSProcess.signal(os_pid, :cont)

  def resume_node(os_pid), do: {:error, {:invalid_os_pid, os_pid}}

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
  @spec fill_memory(GenServer.server(), non_neg_integer()) :: :ok | {:error, term()}
  def fill_memory(server, key_count \\ 10_000)
  def fill_memory(_server, 0), do: :ok

  def fill_memory(server, key_count) when is_integer(key_count) and key_count > 0 do
    value = String.duplicate("x", 1024)

    Enum.reduce_while(1..key_count, :ok, fn i, :ok ->
      case safe_server_run(server, ["SET", "chaos:fill:#{i}", value]) do
        {:ok, _reply} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:fill_failed, i, reason}}}
      end
    end)
  end

  def fill_memory(_server, key_count),
    do: {:error, {:invalid_key_count, key_count}}

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
    case safe_cluster_run(cluster, ["CLUSTER", "KEYSLOT", key]) do
      {:ok, slot_str} ->
        case parse_integer(slot_str) do
          {:ok, slot} -> kill_master(cluster, slot)
          :error -> {:error, {:invalid_keyslot_reply, slot_str}}
        end

      {:error, reason} ->
        {:error, {:keyslot_failed, reason}}
    end
  end

  def kill_master(cluster, slot) when is_integer(slot) and slot in 0..16_383 do
    with {:ok, server_pid} <- find_master_for_slot(cluster, slot),
         :ok <- kill_node(server_pid) do
      {:ok, server_pid}
    end
  end

  def kill_master(_cluster, slot), do: {:error, {:invalid_slot, slot}}

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
  @spec partition(GenServer.server(), [[pid()]]) ::
          {:ok, [pos_integer()]} | {:error, term()}
  def partition(cluster, groups) do
    with {:ok, cluster_nodes} <- cluster_nodes(cluster),
         :ok <- validate_partition_groups(cluster_nodes, groups) do
      [_active | frozen_groups] = groups
      freeze_partition_nodes(List.flatten(frozen_groups))
    end
  end

  @doc """
  Kills a random node in the cluster. Returns `{:ok, server_pid}` of the killed node.
  """
  @spec random_kill(GenServer.server()) :: {:ok, pid()} | {:error, term()}
  def random_kill(cluster) do
    with {:ok, [_node | _rest] = nodes} <- cluster_nodes(cluster),
         selected = Enum.random(nodes),
         :ok <- kill_node(selected) do
      {:ok, selected}
    else
      {:ok, []} -> {:error, :empty_cluster}
      {:error, _reason} = error -> error
    end
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
  Returns a structured error containing every PID that could not be resumed.
  """
  @spec recover(term()) :: :ok | {:error, [{term(), term()}]}
  def recover(os_pids) when is_list(os_pids) do
    failures =
      Enum.flat_map(os_pids, fn os_pid ->
        case resume_node(os_pid) do
          :ok -> []
          {:error, reason} -> [{os_pid, reason}]
        end
      end)

    if failures == [], do: :ok, else: {:error, failures}
  end

  def recover(other), do: {:error, [{other, :invalid_pid_list}]}

  # -------------------------------------------------------------------
  # Internal helpers
  # -------------------------------------------------------------------

  defp resume_after(os_pid, duration_ms) do
    Process.sleep(duration_ms)

    case OSProcess.signal(os_pid, :cont) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Unable to resume paused Redis process #{os_pid}: #{inspect(reason)}")
    end
  end

  defp find_master_for_slot(cluster, target_slot) do
    with {:ok, node_pids} <- cluster_nodes(cluster),
         :ok <- ensure_non_empty_cluster(node_pids) do
      find_master_from_nodes(node_pids, target_slot)
    end
  end

  defp find_master_from_nodes(node_pids, target_slot) do
    cluster_nodes_output =
      Enum.find_value(node_pids, fn pid ->
        case safe_server_run(pid, ["CLUSTER", "NODES"]) do
          {:ok, output} -> output
          _other -> nil
        end
      end)

    with output when is_binary(output) <- cluster_nodes_output,
         {:ok, {host, port}} <- master_addr_for_slot(output, target_slot) do
      match_master_pid(node_pids, host, port)
    else
      nil -> {:error, :no_responsive_nodes}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_non_empty_cluster([]), do: {:error, :empty_cluster}
  defp ensure_non_empty_cluster([_node | _rest]), do: :ok

  defp match_master_pid(node_pids, host, port) do
    infos =
      Enum.flat_map(node_pids, fn pid ->
        case safe_server_info(pid) do
          {:ok, info} -> [{pid, info}]
          {:error, _reason} -> []
        end
      end)

    match =
      Enum.find(infos, fn {_pid, info} -> info.host == host && info.port == port end) ||
        Enum.find(infos, fn {_pid, info} -> info.port == port end)

    case match do
      {pid, _info} -> {:ok, pid}
      nil -> {:error, {:master_pid_not_found, host, port}}
    end
  end

  @doc false
  @spec master_addr_for_slot(String.t(), integer()) ::
          {:ok, {String.t(), :inet.port_number()}} | {:error, term()}
  def master_addr_for_slot(cluster_nodes_output, target_slot)
      when is_binary(cluster_nodes_output) and target_slot in 0..16_383 do
    master =
      cluster_nodes_output
      |> String.split("\n", trim: true)
      |> Enum.find(fn line ->
        case String.split(line) do
          [_id, _address, flags, _master_id, _ping, _pong, _epoch, _state | slots] ->
            master_flags?(flags) && slot_ranges_contain?(slots, target_slot)

          _other ->
            false
        end
      end)

    case master do
      nil ->
        {:error, {:slot_not_found, target_slot}}

      line ->
        [_id, address | _rest] = String.split(line)
        parse_node_address(address)
    end
  end

  def master_addr_for_slot(_cluster_nodes_output, target_slot),
    do: {:error, {:invalid_slot, target_slot}}

  defp master_flags?(flags) do
    flags = String.split(flags, ",")

    "master" in flags &&
      Enum.all?(["fail", "fail?", "handshake", "noaddr"], &(&1 not in flags))
  end

  defp parse_node_address(address) do
    endpoint =
      address
      |> String.split(",", parts: 2)
      |> hd()
      |> String.split("@", parts: 2)
      |> hd()

    case endpoint do
      "[" <> _rest ->
        parse_bracketed_address(endpoint, address)

      _other ->
        parse_unbracketed_address(endpoint, address)
    end
  end

  defp parse_bracketed_address(endpoint, original) do
    case Regex.run(~r/^\[(.+)\]:(\d+)$/, endpoint, capture: :all_but_first) do
      [host, port] -> valid_node_address(host, port, original)
      _other -> {:error, {:invalid_node_address, original}}
    end
  end

  defp parse_unbracketed_address(endpoint, original) do
    case :binary.matches(endpoint, ":") do
      [] ->
        {:error, {:invalid_node_address, original}}

      matches ->
        {separator, 1} = List.last(matches)
        host = binary_part(endpoint, 0, separator)
        port = binary_part(endpoint, separator + 1, byte_size(endpoint) - separator - 1)
        valid_node_address(host, port, original)
    end
  end

  defp valid_node_address("", _port, original),
    do: {:error, {:invalid_node_address, original}}

  defp valid_node_address(host, port, original) do
    case parse_integer(port) do
      {:ok, parsed_port} when parsed_port in 1..65_535 -> {:ok, {host, parsed_port}}
      _other -> {:error, {:invalid_node_address, original}}
    end
  end

  defp slot_ranges_contain?(slot_parts, target_slot) do
    Enum.any?(slot_parts, &slot_range_match?(&1, target_slot))
  end

  # Migrating/importing markers such as [1234->-node-id] describe transitional
  # state, not ownership. Only stable single slots and ranges are considered.
  defp slot_range_match?("[" <> _transition, _target_slot), do: false

  defp slot_range_match?(part, target_slot) do
    case String.split(part, "-", parts: 2) do
      [start_value, end_value] ->
        with {:ok, start_slot} <- parse_slot(start_value),
             {:ok, end_slot} <- parse_slot(end_value) do
          start_slot <= end_slot &&
            target_slot >= start_slot &&
            target_slot <= end_slot
        else
          :error -> false
        end

      [single] ->
        match?({:ok, ^target_slot}, parse_slot(single))
    end
  end

  defp parse_slot(value) do
    case parse_integer(value) do
      {:ok, slot} when slot in 0..16_383 -> {:ok, slot}
      _other -> :error
    end
  end

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _other -> :error
    end
  end

  defp parse_integer(_value), do: :error

  defp server_os_pid(server) do
    with {:ok, info} <- safe_server_info(server),
         {:ok, os_pid} <- fetch_server_os_pid(info) do
      validate_server_os_pid(os_pid)
    end
  end

  defp fetch_server_os_pid(info) do
    case Map.fetch(info, :pid) do
      {:ok, os_pid} -> {:ok, os_pid}
      :error -> {:error, {:invalid_server_info, info}}
    end
  end

  defp validate_server_os_pid(nil), do: {:error, :missing_os_pid}

  defp validate_server_os_pid(os_pid) when is_integer(os_pid) and os_pid > 0 do
    cond do
      not OSProcess.available?("kill") -> {:error, {:executable_not_found, "kill"}}
      OSProcess.alive?(os_pid) -> {:ok, os_pid}
      true -> {:error, {:process_not_alive, os_pid}}
    end
  end

  defp validate_server_os_pid(os_pid), do: {:error, {:invalid_os_pid, os_pid}}

  defp safe_server_info(server) do
    {:ok, Server.info(server)}
  catch
    :exit, reason -> {:error, {:server_unavailable, reason}}
  end

  defp safe_server_run(server, command) do
    Server.run(server, command)
  catch
    :exit, reason -> {:error, {:server_unavailable, reason}}
  end

  defp safe_cluster_run(cluster, command) do
    Cluster.run(cluster, command)
  catch
    :exit, reason -> {:error, {:cluster_unavailable, reason}}
  end

  defp cluster_nodes(cluster) do
    {:ok, Cluster.nodes(cluster)}
  catch
    :exit, reason -> {:error, {:cluster_unavailable, reason}}
  end

  defp validate_partition_groups([], _groups), do: {:error, :empty_cluster}
  defp validate_partition_groups(_cluster_nodes, []), do: {:error, :empty_groups}

  defp validate_partition_groups(cluster_nodes, groups) when is_list(groups) do
    with :ok <- validate_non_empty_groups(groups),
         :ok <- validate_group_members(groups) do
      members = List.flatten(groups)
      unique_members = MapSet.new(members)

      cond do
        length(members) != MapSet.size(unique_members) ->
          {:error, :duplicate_partition_members}

        unique_members != MapSet.new(cluster_nodes) ->
          {:error, {:partition_membership_mismatch, cluster_nodes, members}}

        true ->
          :ok
      end
    end
  end

  defp validate_partition_groups(_cluster_nodes, groups),
    do: {:error, {:invalid_groups, groups}}

  defp validate_non_empty_groups(groups) do
    case Enum.find_index(groups, &(not is_list(&1) or &1 == [])) do
      nil -> :ok
      index -> {:error, {:empty_or_invalid_group, index}}
    end
  end

  defp validate_group_members(groups) do
    case Enum.find_value(Enum.with_index(groups), &invalid_group_member/1) do
      nil -> :ok
      {group_index, member} -> {:error, {:invalid_group_member, group_index, member}}
    end
  end

  defp invalid_group_member({group, group_index}) do
    case Enum.find(group, &(not is_pid(&1))) do
      nil -> nil
      member -> {group_index, member}
    end
  end

  defp freeze_partition_nodes(nodes) do
    Enum.reduce_while(nodes, {:ok, []}, fn server, {:ok, frozen_os_pids} ->
      case freeze_node(server) do
        {:ok, os_pid} ->
          {:cont, {:ok, [os_pid | frozen_os_pids]}}

        {:error, reason} ->
          recovery = recover(frozen_os_pids)
          {:halt, {:error, {:partition_failed, server, reason, recovery}}}
      end
    end)
    |> case do
      {:ok, os_pids} -> {:ok, Enum.reverse(os_pids)}
      {:error, _reason} = error -> error
    end
  end
end
