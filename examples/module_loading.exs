alias RedisServerWrapper.Server

module_path =
  (System.get_env("REDIS_MODULE") ||
     raise("""
     Set REDIS_MODULE to the path of a Redis module.

         REDIS_MODULE=/path/to/module.so \
           mix run examples/module_loading.exs
     """))
  |> Path.expand()

unless File.regular?(module_path) do
  raise "REDIS_MODULE does not point to a file: #{module_path}"
end

module_args =
  System.get_env("REDIS_MODULE_ARGS", "")
  |> OptionParser.split()

port =
  System.get_env("REDIS_PORT", "6460")
  |> String.to_integer()

{:ok, server} =
  Server.start(
    port: port,
    save: :disabled,
    loadmodule: [{module_path, module_args}]
  )

try do
  {:ok, modules} = Server.run(server, ["MODULE", "LIST"])

  case System.get_env("REDIS_MODULE_NAME") do
    nil ->
      :ok

    expected_name ->
      unless String.contains?(modules, expected_name) do
        raise "loaded module list did not contain #{inspect(expected_name)}: #{modules}"
      end
  end

  IO.puts("Loaded Redis module from #{module_path}")
  IO.puts(modules)
after
  if Process.alive?(server), do: Server.stop(server)
end
