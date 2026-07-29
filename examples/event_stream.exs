alias RedisServerWrapper.Server

module_path =
  System.get_env("EVENT_STREAM_MODULE") ||
    raise """
    Set EVENT_STREAM_MODULE to the absolute path of redis-event-stream-module.

        EVENT_STREAM_MODULE=/path/to/libredis_event_stream_module.so \
          mix run examples/event_stream.exs
    """

unless File.regular?(module_path) do
  raise "EVENT_STREAM_MODULE does not point to a file: #{module_path}"
end

port =
  System.get_env("REDIS_PORT", "6460")
  |> String.to_integer()

{:ok, server} =
  RedisServerWrapper.start_server(
    port: port,
    loadmodule: [
      {module_path, ["events", "expired,set", "maxlen", "1000"]}
    ]
  )

try do
  IO.inspect(Server.run(server, ["MODULE", "LIST"]), label: "loaded modules")

  {:ok, "OK"} = Server.run(server, ["SET", "demo:key", "value", "PX", "100"])
  Process.sleep(200)

  # Reading the key forces lazy expiration even on an otherwise idle demo.
  IO.inspect(Server.run(server, ["GET", "demo:key"]), label: "value after expiry")

  IO.inspect(
    Server.run(server, ["XRANGE", "events:expired", "-", "+"]),
    label: "captured expiration events"
  )
after
  if Process.alive?(server), do: Server.stop(server)
end
