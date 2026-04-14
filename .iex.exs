alias RedisServerWrapper, as: RSW
alias RedisServerWrapper.Server
alias RedisServerWrapper.Cluster
alias RedisServerWrapper.Sentinel
alias RedisServerWrapper.Cli
alias RedisServerWrapper.Config
alias RedisServerWrapper.Manager

IO.puts("""

  Redis Server Wrapper - IEx Session
  ===================================

  Aliases loaded:
    RSW      → RedisServerWrapper
    Manager  → RedisServerWrapper.Manager
    Server   → RedisServerWrapper.Server
    Cluster  → RedisServerWrapper.Cluster
    Sentinel → RedisServerWrapper.Sentinel
    Cli      → RedisServerWrapper.Cli
    Config   → RedisServerWrapper.Config

  Manager (persistent, redis-up style):
    Manager.start_basic(port: 6400)
    Manager.start_cluster(masters: 3, base_port: 7100)
    Manager.start_sentinel(master_port: 6390)
    Manager.list()
    Manager.info("redis-basic-1")
    Manager.stop("redis-basic-1")
    Manager.stop_all()
    Manager.cleanup()

  Low-level (GenServer, ephemeral):
    {:ok, s} = RSW.start_server(port: 6400)
    Server.run(s, ["SET", "hello", "world"])
    Server.stop(s)
""")
