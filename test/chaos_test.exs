defmodule RedisServerWrapper.ChaosTest do
  use ExUnit.Case, async: false

  alias RedisServerWrapper.{Chaos, Cli, Cluster, Server}

  defp wait_until(fun, retries \\ 10, delay \\ 1000) do
    if fun.() do
      true
    else
      if retries > 0 do
        Process.sleep(delay)
        wait_until(fun, retries - 1, delay)
      else
        false
      end
    end
  end

  # Direct ping bypassing the GenServer (which may be blocked by a frozen node).
  defp direct_ping(port) do
    cli = Cli.new(port: port)
    Cli.ping(cli)
  end

  # Check if an OS process is in stopped state via ps
  defp process_stopped?(os_pid) do
    case System.cmd("ps", ["-o", "state=", "-p", to_string(os_pid)], stderr_to_stdout: true) do
      {output, 0} -> String.trim(output) =~ "T"
      _ -> false
    end
  end

  # -------------------------------------------------------------------
  # Node-level chaos
  # -------------------------------------------------------------------

  describe "kill_node/1" do
    test "kills the redis-server OS process" do
      {:ok, server} = Server.start_link(port: 6450)
      assert Server.ping(server)

      %{pid: os_pid} = Server.info(server)
      Chaos.kill_node(server)
      Process.sleep(500)

      {_, code} = System.cmd("kill", ["-0", to_string(os_pid)], stderr_to_stdout: true)
      assert code != 0

      Server.stop(server)
    end
  end

  describe "freeze_node/1 and resume_node/1" do
    test "freezes and resumes a node" do
      {:ok, server} = Server.start_link(port: 6451)
      assert Server.ping(server)

      {:ok, os_pid} = Chaos.freeze_node(server)
      Process.sleep(200)

      # Verify the OS process is in stopped state
      assert process_stopped?(os_pid)

      # Resume and verify it responds again
      Chaos.resume_node(os_pid)
      assert wait_until(fn -> direct_ping(6451) end, 5, 500)

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "pause_node/2" do
    test "freezes a node for specified duration then auto-resumes" do
      {:ok, server} = Server.start_link(port: 6452)
      assert Server.ping(server)

      {:ok, os_pid} = Chaos.pause_node(server, 2_000)
      Process.sleep(200)

      # Verify frozen via OS state
      assert process_stopped?(os_pid)

      # Wait for auto-resume
      Process.sleep(3_000)
      assert wait_until(fn -> direct_ping(6452) end, 5, 500)

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "slow_down/2" do
    test "pauses client connections for a duration" do
      {:ok, server} = Server.start_link(port: 6453)
      assert Server.ping(server)

      assert {:ok, "OK"} = Chaos.slow_down(server, 1_000)

      # After the pause expires, commands should work normally
      Process.sleep(1_500)
      assert {:ok, "OK"} = Server.run(server, ["SET", "k", "v"])

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "flushall/1" do
    test "wipes all data" do
      {:ok, server} = Server.start_link(port: 6454)
      assert {:ok, "OK"} = Server.run(server, ["SET", "key1", "val1"])
      assert {:ok, "OK"} = Server.run(server, ["SET", "key2", "val2"])
      assert {:ok, "2"} = Server.run(server, ["DBSIZE"])

      assert {:ok, "OK"} = Chaos.flushall(server)
      assert {:ok, "0"} = Server.run(server, ["DBSIZE"])

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "fill_memory/2" do
    test "fills node with dummy keys" do
      {:ok, server} = Server.start_link(port: 6455)

      Chaos.fill_memory(server, 100)

      {:ok, count} = Server.run(server, ["DBSIZE"])
      assert String.to_integer(count) == 100

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "trigger_save/1" do
    test "triggers a BGSAVE" do
      {:ok, server} = Server.start_link(port: 6456)
      assert {:ok, "Background saving started"} = Chaos.trigger_save(server)

      Server.stop(server)
      Process.sleep(500)
    end
  end

  # -------------------------------------------------------------------
  # Cluster-level chaos
  # -------------------------------------------------------------------

  describe "kill_master/2" do
    @tag timeout: 30_000
    test "kills the master owning a key's slot" do
      {:ok, cluster} = Cluster.start_link(masters: 3, base_port: 7300)
      assert Cluster.healthy?(cluster)

      assert {:ok, "OK"} = Cluster.run(cluster, ["-c", "SET", "testkey", "value"])

      {:ok, killed_pid} = Chaos.kill_master(cluster, "testkey")
      assert is_pid(killed_pid)

      Process.sleep(500)
      refute Server.alive?(killed_pid)

      Cluster.stop(cluster)
      Process.sleep(1000)
    end

    @tag timeout: 30_000
    test "kills the master owning a specific slot" do
      {:ok, cluster} = Cluster.start_link(masters: 3, base_port: 7310)
      assert Cluster.healthy?(cluster)

      {:ok, killed_pid} = Chaos.kill_master(cluster, 0)
      assert is_pid(killed_pid)

      Process.sleep(500)
      refute Server.alive?(killed_pid)

      Cluster.stop(cluster)
      Process.sleep(1000)
    end
  end

  describe "partition/2" do
    @tag timeout: 60_000
    test "freezes nodes in non-active groups" do
      {:ok, cluster} = Cluster.start_link(masters: 3, base_port: 7320)
      assert Cluster.healthy?(cluster)

      nodes = Cluster.nodes(cluster)
      {active, frozen} = Enum.split(nodes, 1)

      # Get ports and OS pids before partition (GenServer will be blocked after freeze)
      frozen_info =
        Enum.map(frozen, fn pid ->
          info = Server.info(pid)
          {info.port, info.pid}
        end)

      active_ports = Enum.map(active, fn pid -> Server.info(pid).port end)

      {:ok, frozen_os_pids} = Chaos.partition(cluster, [active, frozen])
      Process.sleep(500)

      # Active nodes should respond
      Enum.each(active_ports, fn port ->
        assert direct_ping(port)
      end)

      # Frozen nodes should be in stopped state
      Enum.each(frozen_info, fn {_port, os_pid} ->
        assert process_stopped?(os_pid)
      end)

      # Recover using returned OS pids
      Chaos.recover(frozen_os_pids)

      # Wait for all nodes to become responsive
      all_ports = active_ports ++ Enum.map(frozen_info, &elem(&1, 0))
      assert wait_until(fn -> Enum.all?(all_ports, &direct_ping/1) end, 10, 1000)

      Cluster.stop(cluster)
      Process.sleep(1000)
    end
  end

  describe "random_kill/1" do
    @tag timeout: 30_000
    test "kills a random cluster node" do
      {:ok, cluster} = Cluster.start_link(masters: 3, base_port: 7330)
      assert Cluster.healthy?(cluster)

      {:ok, killed_pid} = Chaos.random_kill(cluster)
      assert is_pid(killed_pid)
      Process.sleep(500)

      refute Server.alive?(killed_pid)

      Cluster.stop(cluster)
      Process.sleep(1000)
    end
  end

  describe "trigger_failover/2" do
    @tag timeout: 60_000
    test "falls back to FORCE when master is dead" do
      {:ok, cluster} =
        Cluster.start_link(masters: 3, replicas_per_master: 1, base_port: 7350)

      assert Cluster.healthy?(cluster)

      # Kill the master for slot 0
      {:ok, killed} = Chaos.kill_master(cluster, 0)
      Process.sleep(1000)

      # Find a replica whose master was killed
      nodes = Cluster.nodes(cluster)
      remaining = Enum.reject(nodes, &(&1 == killed))

      replica =
        Enum.find(remaining, fn pid ->
          case Server.run(pid, ["CLUSTER", "NODES"]) do
            {:ok, output} ->
              # Find this node's own line (contains "myself")
              output
              |> String.split("\n", trim: true)
              |> Enum.any?(fn line ->
                String.contains?(line, "myself") && String.contains?(line, "slave")
              end)

            _ ->
              false
          end
        end)

      if replica do
        # This should auto-fallback to FORCE since master is dead
        assert {:ok, _} = Chaos.trigger_failover(replica)
      end

      Cluster.stop(cluster)
      Process.sleep(1000)
    end

    @tag timeout: 30_000
    test "force: true sends FORCE directly" do
      {:ok, cluster} =
        Cluster.start_link(masters: 3, replicas_per_master: 1, base_port: 7360)

      assert Cluster.healthy?(cluster)

      # Find any replica node
      nodes = Cluster.nodes(cluster)

      replica =
        Enum.find(nodes, fn pid ->
          case Server.run(pid, ["CLUSTER", "NODES"]) do
            {:ok, output} ->
              output
              |> String.split("\n", trim: true)
              |> Enum.any?(fn line ->
                String.contains?(line, "myself") && String.contains?(line, "slave")
              end)

            _ ->
              false
          end
        end)

      assert replica != nil
      assert {:ok, _} = Chaos.trigger_failover(replica, force: true)

      Cluster.stop(cluster)
      Process.sleep(1000)
    end
  end

  describe "recover/1" do
    @tag timeout: 60_000
    test "resumes all frozen nodes in a cluster" do
      {:ok, cluster} = Cluster.start_link(masters: 3, base_port: 7340)
      assert Cluster.healthy?(cluster)

      # Get ports before freezing
      ports =
        Enum.map(Cluster.nodes(cluster), fn pid ->
          Server.info(pid).port
        end)

      # Freeze all nodes and collect OS pids
      os_pids =
        Enum.map(Cluster.nodes(cluster), fn pid ->
          {:ok, os_pid} = Chaos.freeze_node(pid)
          os_pid
        end)

      Process.sleep(500)

      # Verify all frozen
      Enum.each(os_pids, fn os_pid ->
        assert process_stopped?(os_pid)
      end)

      # Recover all
      Chaos.recover(os_pids)

      # Wait for all nodes to become responsive
      assert wait_until(fn -> Enum.all?(ports, &direct_ping/1) end, 10, 1000)

      Cluster.stop(cluster)
      Process.sleep(1000)
    end
  end
end
