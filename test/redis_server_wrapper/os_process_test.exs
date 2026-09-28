defmodule RedisServerWrapper.OSProcessTest do
  use ExUnit.Case, async: true

  alias RedisServerWrapper.OSProcess

  # HARD CONSTRAINT: this module never signals a real process. Every case
  # here exercises the pure argv builders (signal_args/2, group_signal_args/2)
  # or asserts that signal/2 and signal_group/2 reject an invalid pid before
  # they ever reach `kill`. No valid pgid is ever passed to signal_group/2.

  @invalid_pids [0, 1, -1, -12_345, 2_147_483_648]

  describe "group_signal_args/2" do
    test "builds argv with -- before the negative pgid" do
      assert OSProcess.group_signal_args(12_345, :kill) == {:ok, ["-9", "--", "-12345"]}
      assert OSProcess.group_signal_args(12_345, :term) == {:ok, ["-TERM", "--", "-12345"]}
    end

    test "rejects pids outside the valid range" do
      for pid <- @invalid_pids do
        assert OSProcess.group_signal_args(pid, :kill) == {:error, {:invalid_pid, pid}}
      end
    end
  end

  describe "signal_args/2" do
    test "builds argv for a plain pid" do
      assert OSProcess.signal_args(12_345, :kill) == {:ok, ["-9", "12345"]}
    end

    test "rejects pids outside the valid range" do
      for pid <- @invalid_pids do
        assert OSProcess.signal_args(pid, :kill) == {:error, {:invalid_pid, pid}}
      end
    end
  end

  describe "process_group/1" do
    # Read-only lookups: these never signal anything.
    test "reports the process group of the running BEAM" do
      assert {:ok, pgid} = OSProcess.process_group(String.to_integer(System.pid()))
      assert is_integer(pgid) and pgid > 0
    end

    test "returns :error for a pid that does not exist" do
      assert OSProcess.process_group(2_147_483_000) == :error
    end
  end

  describe "signal/2 with an invalid pid" do
    test "returns {:error, {:invalid_pid, _}} without touching kill" do
      for pid <- @invalid_pids do
        assert OSProcess.signal(pid, :kill) == {:error, {:invalid_pid, pid}}
        assert OSProcess.signal(pid, :term) == {:error, {:invalid_pid, pid}}
      end
    end
  end

  describe "signal_group/2 with an invalid pgid" do
    test "returns {:error, {:invalid_pid, _}} without touching kill" do
      for pgid <- @invalid_pids do
        assert OSProcess.signal_group(pgid, :kill) == {:error, {:invalid_pid, pgid}}
        assert OSProcess.signal_group(pgid, :term) == {:error, {:invalid_pid, pgid}}
      end
    end
  end
end
