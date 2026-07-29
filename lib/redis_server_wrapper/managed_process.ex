defmodule RedisServerWrapper.ManagedProcess do
  @moduledoc false

  use GenServer

  alias RedisServerWrapper.OSProcess

  require Logger

  @compile {:no_warn_undefined, Forcola.Daemon}

  defstruct [
    :backend,
    :daemon,
    :label,
    :os_pid,
    :port_ref,
    :shutdown
  ]

  @type backend :: true | :forcola

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec info(GenServer.server()) :: %{backend: backend(), pid: pos_integer() | nil}
  def info(server), do: GenServer.call(server, :info)

  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server, :normal)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    backend = Keyword.fetch!(opts, :backend)
    argv = Keyword.fetch!(opts, :argv)
    ready = Keyword.fetch!(opts, :ready)
    timeout = Keyword.fetch!(opts, :ready_timeout_ms)
    label = Keyword.get(opts, :label, "managed process")
    pid_path = Keyword.get(opts, :pid_path)
    shutdown = Keyword.get(opts, :shutdown, fn -> :ok end)

    with :ok <- validate_backend(backend),
         {:ok, executable, args} <- resolve_argv(argv) do
      start_backend(backend, executable, args, ready, timeout, label, pid_path, shutdown)
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:info, _from, state) do
    {:reply, %{backend: state.backend, pid: state.os_pid}, state}
  end

  @impl true
  def handle_info({:EXIT, daemon, reason}, %{daemon: daemon} = state)
      when is_pid(daemon) do
    stop_reason = {:managed_process_exit, state.label, :forcola, reason}
    Logger.warning("#{state.label} exited under Forcola: #{inspect(reason)}")
    {:stop, stop_reason, %{state | daemon: nil, os_pid: nil}}
  end

  def handle_info({port_ref, {:exit_status, status}}, %{port_ref: port_ref} = state)
      when is_port(port_ref) do
    stop_reason = {:managed_process_exit, state.label, :port, {:exit_status, status}}
    Logger.warning("#{state.label} exited with status #{status}")
    {:stop, stop_reason, %{state | port_ref: nil, os_pid: nil}}
  end

  def handle_info({:EXIT, port_ref, reason}, %{port_ref: port_ref} = state)
      when is_port(port_ref) do
    stop_reason = {:managed_process_exit, state.label, :port, reason}
    Logger.warning("#{state.label} port exited: #{inspect(reason)}")
    {:stop, stop_reason, %{state | port_ref: nil, os_pid: nil}}
  end

  def handle_info({port_ref, {:data, {:eol, line}}}, %{port_ref: port_ref} = state)
      when is_port(port_ref) do
    Logger.debug("#{state.label}: #{line}")
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{daemon: daemon}) when is_pid(daemon) do
    if Process.alive?(daemon) do
      GenServer.stop(daemon, :normal, :infinity)
    end

    :ok
  catch
    :exit, _reason -> :ok
  end

  def terminate(_reason, state) do
    safe_shutdown(state.shutdown)
    await_process_exit(state.os_pid, 500)
    safe_port_close(state.port_ref)

    if OSProcess.alive?(state.os_pid) do
      _result = OSProcess.signal(state.os_pid, :kill)
    end

    :ok
  end

  defp start_backend(true, executable, args, ready, timeout, label, _pid_path, shutdown) do
    port_ref =
      Port.open({:spawn_executable, executable}, [
        {:args, args},
        :binary,
        :exit_status,
        {:line, 1024}
      ])

    case await_ready(ready, timeout, fn -> Port.info(port_ref) != nil end) do
      :ok ->
        {:os_pid, os_pid} = Port.info(port_ref, :os_pid)

        {:ok,
         %__MODULE__{
           backend: true,
           label: label,
           os_pid: os_pid,
           port_ref: port_ref,
           shutdown: shutdown
         }}

      {:error, reason} ->
        safe_port_close(port_ref)
        {:stop, {:managed_process_start_failed, label, reason}}
    end
  rescue
    error in ArgumentError ->
      {:stop, {:managed_process_start_failed, label, Exception.message(error)}}
  end

  defp start_backend(:forcola, executable, args, ready, timeout, label, pid_path, shutdown) do
    if Code.ensure_loaded?(Forcola.Daemon) do
      daemon_opts = [
        argv: [executable | args],
        ready: ready,
        ready_timeout_ms: timeout,
        output: :logger,
        log_output: :debug,
        log_prefix: "#{label}: "
      ]

      case Forcola.Daemon.start_link(daemon_opts) do
        {:ok, daemon} ->
          {:ok,
           %__MODULE__{
             backend: :forcola,
             daemon: daemon,
             label: label,
             os_pid: read_pidfile(pid_path),
             shutdown: shutdown
           }}

        {:error, reason} ->
          {:stop, {:managed_process_start_failed, label, reason}}
      end
    else
      {:stop, :forcola_not_available}
    end
  end

  defp validate_backend(backend) when backend in [true, :forcola], do: :ok
  defp validate_backend(backend), do: {:error, {:invalid_managed, backend}}

  defp resolve_argv([executable | args]) when is_binary(executable) and is_list(args) do
    case System.find_executable(executable) do
      nil -> {:error, {:executable_not_found, executable}}
      path -> {:ok, path, Enum.map(args, &to_string/1)}
    end
  end

  defp resolve_argv(argv), do: {:error, {:invalid_argv, argv}}

  defp await_ready(ready, timeout, alive?) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_ready(ready, deadline, alive?)
  end

  defp do_await_ready(ready, deadline, alive?) do
    cond do
      not alive?.() ->
        {:error, :exited_before_ready}

      ready.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :ready_timeout}

      true ->
        Process.sleep(50)
        do_await_ready(ready, deadline, alive?)
    end
  end

  defp safe_shutdown(shutdown) do
    shutdown.()
    :ok
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp await_process_exit(nil, _timeout), do: :ok

  defp await_process_exit(pid, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_process_exit(pid, deadline)
  end

  defp do_await_process_exit(pid, deadline) do
    cond do
      not OSProcess.alive?(pid) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :timeout

      true ->
        Process.sleep(25)
        do_await_process_exit(pid, deadline)
    end
  end

  defp safe_port_close(port_ref) when is_port(port_ref) do
    if Port.info(port_ref) != nil, do: Port.close(port_ref)
  rescue
    ArgumentError -> :ok
  end

  defp safe_port_close(_port_ref), do: :ok

  defp read_pidfile(nil), do: nil

  defp read_pidfile(path) do
    with {:ok, content} <- File.read(path),
         {pid, ""} <- content |> String.trim() |> Integer.parse(),
         true <- pid > 0 do
      pid
    else
      _other -> nil
    end
  end
end
