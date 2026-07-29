defmodule RedisServerWrapper.OSProcess do
  @moduledoc false

  @type signal :: :term | :kill | :stop | :cont
  @type command_error ::
          {:executable_not_found, String.t()}
          | {:signal_failed, signal(), integer(), non_neg_integer(), String.t()}

  @spec available?(String.t()) :: boolean()
  def available?(executable), do: System.find_executable(executable) != nil

  @spec signal(integer(), signal()) :: :ok | {:error, command_error()}
  def signal(pid, signal) when is_integer(pid) do
    case System.find_executable("kill") do
      nil ->
        {:error, {:executable_not_found, "kill"}}

      kill ->
        case System.cmd(kill, [signal_arg(signal), to_string(pid)], stderr_to_stdout: true) do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, {:signal_failed, signal, pid, status, String.trim(output)}}
        end
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

  defp parse_pid(value) do
    case Integer.parse(value) do
      {pid, ""} when pid > 0 -> [pid]
      _other -> []
    end
  end
end
