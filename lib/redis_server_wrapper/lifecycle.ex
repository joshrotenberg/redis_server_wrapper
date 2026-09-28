defmodule RedisServerWrapper.Lifecycle do
  @moduledoc false

  # Shared idempotent stop for Server, Cluster, Sentinel and ManagedProcess.
  # GenServer.stop/2 exits the caller with :noproc when the target is a dead
  # pid, an unregistered name, or dies during the call. A lifecycle stop
  # should be safe to call more than once, so those cases return :ok. Any
  # other exit reason still propagates.

  @spec stop_process(GenServer.server()) :: :ok
  def stop_process(server) do
    GenServer.stop(server, :normal)
  catch
    :exit, :noproc -> :ok
    :exit, {:noproc, _} -> :ok
  end
end
