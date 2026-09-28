defmodule RedisServerWrapper.OSProcess do
  @moduledoc false

  @type signal :: :term | :kill | :stop | :cont
  @type command_error ::
          {:executable_not_found, String.t()}
          | {:signal_failed, signal(), integer(), non_neg_integer(), String.t()}
          | {:invalid_pid, integer()}

  # Valid POSIX pid/pgid range. Rejecting anything outside this range keeps a
  # negative, zero, or out-of-range value from ever reaching argv, where a
  # leading "-" can be misparsed by `kill` as an option rather than a pid.
  @min_pid 2
  @max_pid 2_147_483_647

  @spec available?(String.t()) :: boolean()
  def available?(executable), do: System.find_executable(executable) != nil

  @spec signal(integer(), signal()) :: :ok | {:error, command_error()}
  def signal(pid, signal) when is_integer(pid) do
    case signal_args(pid, signal) do
      {:ok, argv} -> run_kill(argv, signal, pid)
      {:error, reason} -> {:error, reason}
    end
  end

  @spec signal_group(integer(), signal()) :: :ok | {:error, command_error()}
  def signal_group(pgid, signal) when is_integer(pgid) do
    case group_signal_args(pgid, signal) do
      {:ok, argv} -> run_kill(argv, signal, pgid)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec signal_args(integer(), signal()) ::
          {:ok, [String.t()]} | {:error, {:invalid_pid, integer()}}
  def signal_args(pid, signal) when is_integer(pid) do
    if valid_pid?(pid) do
      {:ok, [signal_arg(signal), to_string(pid)]}
    else
      {:error, {:invalid_pid, pid}}
    end
  end

  @doc false
  @spec group_signal_args(integer(), signal()) ::
          {:ok, [String.t()]} | {:error, {:invalid_pid, integer()}}
  def group_signal_args(pgid, signal) when is_integer(pgid) do
    if valid_pid?(pgid) do
      {:ok, [signal_arg(signal), "--", "-#{pgid}"]}
    else
      {:error, {:invalid_pid, pgid}}
    end
  end

  @spec alive?(integer() | nil) :: boolean()
  def alive?(nil), do: false

  def alive?(pid) when is_integer(pid) and pid > 0 do
    case System.find_executable("kill") do
      nil ->
        procfs_pid_alive?(pid)

      kill ->
        case System.cmd(kill, ["-0", to_string(pid)], stderr_to_stdout: true) do
          {_output, 0} -> true
          _other -> false
        end
    end
  end

  def alive?(_pid), do: false

  @spec orphaned?(pos_integer()) :: boolean()
  def orphaned?(pid) when is_integer(pid) and pid > 0 do
    case System.find_executable("ps") do
      nil ->
        procfs_parent_pid(pid) == 1

      ps ->
        case System.cmd(ps, ["-o", "ppid=", "-p", to_string(pid)], stderr_to_stdout: true) do
          {output, 0} -> String.trim(output) == "1"
          _other -> false
        end
    end
  end

  @spec process_group(pos_integer()) :: {:ok, pos_integer()} | :error
  def process_group(pid) when is_integer(pid) and pid > 0 do
    case System.find_executable("ps") do
      nil -> process_group_from_procfs(pid)
      ps -> process_group_from_ps(ps, pid)
    end
  end

  @spec pids_on_port(:inet.port_number()) ::
          {:ok, [pos_integer()]} | {:error, {:executable_not_found, String.t()}}
  def pids_on_port(port) when is_integer(port) and port >= 0 and port <= 65_535 do
    case System.find_executable("lsof") do
      nil ->
        {:error, {:executable_not_found, "lsof"}}

      lsof ->
        case System.cmd(lsof, ["-ti", ":#{port}"], stderr_to_stdout: true) do
          {output, 0} ->
            pids =
              output
              |> String.split(~r/\s+/, trim: true)
              |> Enum.flat_map(&parse_pid/1)

            {:ok, pids}

          _other ->
            {:ok, []}
        end
    end
  end

  @spec pids_on_socket(String.t()) ::
          {:ok, [pos_integer()]} | {:error, {:executable_not_found, String.t()}}
  def pids_on_socket(path) when is_binary(path) and path != "" do
    case System.find_executable("lsof") do
      nil ->
        {:error, {:executable_not_found, "lsof"}}

      lsof ->
        case System.cmd(lsof, ["-t", "--", path], stderr_to_stdout: true) do
          {output, 0} ->
            pids =
              output
              |> String.split(~r/\s+/, trim: true)
              |> Enum.flat_map(&parse_pid/1)

            {:ok, pids}

          _other ->
            {:ok, []}
        end
    end
  end

  defp signal_arg(:term), do: "-TERM"
  defp signal_arg(:kill), do: "-9"
  defp signal_arg(:stop), do: "-STOP"
  defp signal_arg(:cont), do: "-CONT"

  defp valid_pid?(pid), do: pid >= @min_pid and pid <= @max_pid

  defp run_kill(argv, signal, pid) do
    case System.find_executable("kill") do
      nil ->
        {:error, {:executable_not_found, "kill"}}

      kill ->
        case System.cmd(kill, argv, stderr_to_stdout: true) do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, {:signal_failed, signal, pid, status, String.trim(output)}}
        end
    end
  end

  defp process_group_from_procfs(pid) do
    case procfs_pgid(pid) do
      pgid when is_integer(pgid) -> {:ok, pgid}
      nil -> :error
    end
  end

  defp process_group_from_ps(ps, pid) do
    case System.cmd(ps, ["-o", "pgid=", "-p", to_string(pid)], stderr_to_stdout: true) do
      {output, 0} -> parse_pgid(output)
      _other -> :error
    end
  end

  defp parse_pgid(output) do
    case Integer.parse(String.trim(output)) do
      {pgid, ""} -> {:ok, pgid}
      _other -> :error
    end
  end

  defp procfs_pid_alive?(pid) do
    File.dir?("/proc") and File.exists?("/proc/#{pid}")
  end

  defp procfs_parent_pid(pid) do
    with {:ok, status} <- File.read("/proc/#{pid}/status"),
         line when is_binary(line) <-
           Enum.find(String.split(status, "\n"), &String.starts_with?(&1, "PPid:")),
         {parent_pid, _rest} <- Integer.parse(String.trim_leading(line, "PPid:") |> String.trim()) do
      parent_pid
    else
      _other -> nil
    end
  end

  defp procfs_pgid(pid) do
    with {:ok, contents} <- File.read("/proc/#{pid}/stat"),
         [_pid_and_comm, rest] <- String.split(contents, ")", parts: 2),
         fields <- rest |> String.trim() |> String.split(" ", trim: true),
         pgid_str when is_binary(pgid_str) <- Enum.at(fields, 2),
         {pgid, _rest} <- Integer.parse(pgid_str) do
      pgid
    else
      _other -> nil
    end
  end

  defp parse_pid(value) do
    case Integer.parse(value) do
      {pid, ""} when pid > 0 -> [pid]
      _other -> []
    end
  end
end
