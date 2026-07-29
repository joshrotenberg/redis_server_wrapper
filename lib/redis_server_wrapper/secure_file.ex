defmodule RedisServerWrapper.SecureFile do
  @moduledoc false

  @directory_mode 0o700
  @file_mode 0o600

  @spec make_private_directory!(Path.t()) :: :ok
  def make_private_directory!(path) do
    File.mkdir_p!(path)
    File.chmod!(path, @directory_mode)
  end

  @spec write_private!(Path.t(), iodata()) :: :ok
  def write_private!(path, contents) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} ->
        File.chmod!(path, @file_mode)
        write_existing!(path, contents)

      {:ok, %File.Stat{type: type}} ->
        raise ArgumentError,
              "refusing to write private data to non-regular #{type}: #{path}"

      {:error, :enoent} ->
        create_private!(path, contents)

      {:error, reason} ->
        raise File.Error, reason: reason, action: "inspect private file", path: path
    end
  end

  @spec atomic_write_private!(Path.t(), iodata()) :: :ok
  def atomic_write_private!(path, contents) do
    suffix = System.unique_integer([:positive, :monotonic])
    temp_path = Path.join(Path.dirname(path), ".#{Path.basename(path)}.tmp-#{suffix}")

    try do
      create_private!(temp_path, contents)
      File.rename!(temp_path, path)
      File.chmod!(path, @file_mode)
    after
      File.rm(temp_path)
    end
  end

  @spec harden_private_file(Path.t()) ::
          :ok | :missing | {:error, {:not_a_regular_file, atom()}} | {:error, term()}
  def harden_private_file(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} ->
        File.chmod(path, @file_mode)

      {:ok, %File.Stat{type: type}} ->
        {:error, {:not_a_regular_file, type}}

      {:error, :enoent} ->
        :missing

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_private!(path, contents) do
    File.open!(path, [:write, :exclusive, :binary], fn io ->
      File.chmod!(path, @file_mode)
      IO.binwrite(io, contents)
      :ok = :file.sync(io)
    end)
  end

  defp write_existing!(path, contents) do
    File.open!(path, [:write, :binary], fn io ->
      IO.binwrite(io, contents)
      :ok = :file.sync(io)
    end)
  end
end
