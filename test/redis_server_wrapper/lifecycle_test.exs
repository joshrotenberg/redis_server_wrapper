defmodule RedisServerWrapper.LifecycleTest do
  use ExUnit.Case, async: true

  alias RedisServerWrapper.{Cluster, Lifecycle, ManagedProcess, Sentinel, Server}

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000
    pid
  end

  test "stop_process/1 returns :ok for a dead pid" do
    assert Lifecycle.stop_process(dead_pid()) == :ok
  end

  test "stop_process/1 returns :ok for an unregistered name" do
    assert Lifecycle.stop_process(:redis_server_wrapper_no_such_process) == :ok
    assert Lifecycle.stop_process({:global, :redis_server_wrapper_no_such_process}) == :ok
  end

  test "stop_process/1 stops a live GenServer and is idempotent" do
    {:ok, pid} = Agent.start(fn -> :ok end)

    assert Lifecycle.stop_process(pid) == :ok
    refute Process.alive?(pid)
    assert Lifecycle.stop_process(pid) == :ok
  end

  test "public stop/1 functions return :ok for a dead pid" do
    for module <- [Server, Cluster, Sentinel, ManagedProcess] do
      assert module.stop(dead_pid()) == :ok
    end
  end
end
