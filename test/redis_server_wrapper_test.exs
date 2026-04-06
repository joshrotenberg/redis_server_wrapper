defmodule RedisServerWrapperTest do
  use ExUnit.Case, async: false

  alias RedisServerWrapper.{Cli, Cluster, Config, Sentinel, Server}

  defp wait_until(fun, retries \\ 10, delay \\ 1000) do
    if fun.() do
      true
    else
      if retries > 0 do
        Process.sleep(delay)
        wait_until(fun, retries - 1, delay)
      else
        false
      end
    end
  end

  # -------------------------------------------------------------------
  # Config tests (no redis-server needed)
  # -------------------------------------------------------------------

  describe "Config" do
    test "generates basic config string" do
      config = Config.new(port: 6400, bind: "127.0.0.1", password: "secret")
      output = Config.to_config_string(config)

      assert output =~ "port 6400"
      assert output =~ "bind 127.0.0.1"
      assert output =~ "requirepass secret"
      assert output =~ "daemonize no"
      assert output =~ "appendonly no"
    end

    test "omits nil fields" do
      config = Config.new()
      output = Config.to_config_string(config)

      refute output =~ "requirepass"
      refute output =~ "maxmemory"
      refute output =~ "unixsocket"
    end

    test "handles save policies" do
      disabled = Config.new(save: :disabled) |> Config.to_config_string()
      assert disabled =~ ~s(save "")

      custom = Config.new(save: [{900, 1}, {300, 10}]) |> Config.to_config_string()
      assert custom =~ "save 900 1"
      assert custom =~ "save 300 10"

      default = Config.new() |> Config.to_config_string()
      refute default =~ "save"
    end

    test "cluster config directives" do
      config =
        Config.new(
          cluster_enabled: true,
          cluster_config_file: "nodes.conf",
          cluster_node_timeout: 5000
        )

      output = Config.to_config_string(config)
      assert output =~ "cluster-enabled yes"
      assert output =~ "cluster-config-file nodes.conf"
      assert output =~ "cluster-node-timeout 5000"
    end

    test "replication directives" do
      config = Config.new(replicaof: {"127.0.0.1", 6379}, masterauth: "secret")
      output = Config.to_config_string(config)
      assert output =~ "replicaof 127.0.0.1 6379"
      assert output =~ "masterauth secret"
    end

    test "extra directives" do
      config = Config.new(extra: [{"maxmemory-policy", "allkeys-lru"}, {"hz", "20"}])
      output = Config.to_config_string(config)
      assert output =~ "maxmemory-policy allkeys-lru"
      assert output =~ "hz 20"
    end

    test "TLS config" do
      config =
        Config.new(
          tls_port: 6380,
          tls_cert_file: "/path/cert.pem",
          tls_key_file: "/path/key.pem",
          tls_ca_cert_file: "/path/ca.pem"
        )

      output = Config.to_config_string(config)
      assert output =~ "tls-port 6380"
      assert output =~ "tls-cert-file /path/cert.pem"
      assert output =~ "tls-key-file /path/key.pem"
      assert output =~ "tls-ca-cert-file /path/ca.pem"
    end
  end

  # -------------------------------------------------------------------
  # Integration tests (require redis-server on PATH)
  # -------------------------------------------------------------------

  describe "RedisServerWrapper" do
    test "available? returns true when redis-server is on PATH" do
      assert RedisServerWrapper.available?()
    end

    test "version returns redis version string" do
      assert {:ok, version} = RedisServerWrapper.version()
      assert version =~ ~r/^\d+\.\d+/
    end
  end

  describe "Server" do
    test "start, ping, run commands, stop" do
      {:ok, server} = Server.start_link(port: 6400)

      assert Server.ping(server)
      assert Server.alive?(server)

      info = Server.info(server)
      assert info.port == 6400
      assert info.pid != nil
      assert info.detached == false

      assert {:ok, "OK"} = Server.run(server, ["SET", "mykey", "myvalue"])
      assert {:ok, "myvalue"} = Server.run(server, ["GET", "mykey"])

      Server.stop(server)
      Process.sleep(1000)
    end

    test "start with password" do
      {:ok, server} = Server.start_link(port: 6401, password: "testsecret")

      assert Server.ping(server)
      assert {:ok, "OK"} = Server.run(server, ["SET", "k", "v"])
      assert {:ok, "v"} = Server.run(server, ["GET", "k"])

      Server.stop(server)
      Process.sleep(500)
    end

    test "detach prevents shutdown on stop (unmanaged)" do
      {:ok, server} = Server.start_link(port: 6402, managed: false)
      info = Server.info(server)
      os_pid = info.pid

      assert :ok = Server.detach(server)
      Server.stop(server)

      # The OS process should still be alive
      Process.sleep(500)
      {_, code} = System.cmd("kill", ["-0", to_string(os_pid)], stderr_to_stdout: true)
      assert code == 0

      # Clean up manually
      cli = Cli.new(port: 6402)
      Cli.shutdown(cli)
      Process.sleep(500)
    end

    test "detach returns error in managed mode" do
      {:ok, server} = Server.start_link(port: 6404)

      assert {:error, :managed_server} = Server.detach(server)

      Server.stop(server)
      Process.sleep(500)
    end

    test "cli returns usable Cli struct" do
      {:ok, server} = Server.start_link(port: 6403)

      cli = Server.cli(server)
      assert %Cli{} = cli
      assert Cli.ping(cli)

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "Server managed mode" do
    test "managed server dies when GenServer stops" do
      {:ok, server} = Server.start_link(port: 6410, managed: true)
      assert Server.ping(server)

      info = Server.info(server)
      assert info.managed == true
      os_pid = info.pid
      assert os_pid != nil

      Server.stop(server)
      Process.sleep(1000)

      # The OS process should be gone
      {_, code} = System.cmd("kill", ["-0", to_string(os_pid)], stderr_to_stdout: true)
      assert code != 0
    end

    test "managed server runs commands" do
      {:ok, server} = Server.start_link(port: 6411)

      assert {:ok, "OK"} = Server.run(server, ["SET", "managed_key", "managed_val"])
      assert {:ok, "managed_val"} = Server.run(server, ["GET", "managed_key"])

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "Server unmanaged mode" do
    test "unmanaged server uses daemonize" do
      {:ok, server} = Server.start_link(port: 6412, managed: false)
      assert Server.ping(server)

      info = Server.info(server)
      assert info.managed == false
      assert info.pid != nil

      Server.stop(server)
      Process.sleep(1000)
    end

    test "unmanaged server runs commands" do
      {:ok, server} = Server.start_link(port: 6413, managed: false)

      assert {:ok, "OK"} = Server.run(server, ["SET", "unmanaged_key", "val"])
      assert {:ok, "val"} = Server.run(server, ["GET", "unmanaged_key"])

      Server.stop(server)
      Process.sleep(500)
    end
  end

  describe "Cluster" do
    @tag timeout: 30_000
    test "start 3-master cluster, verify health, stop" do
      {:ok, cluster} = Cluster.start_link(masters: 3, base_port: 7100)

      assert Cluster.all_alive?(cluster)
      assert Cluster.healthy?(cluster)

      info = Cluster.info(cluster)
      assert info.masters == 3
      assert info.total_nodes == 3
      assert length(info.node_addrs) == 3

      addr = Cluster.addr(cluster)
      assert addr == "127.0.0.1:7100"

      Cluster.stop(cluster)
      Process.sleep(1000)
    end

    @tag timeout: 30_000
    test "cluster with replicas" do
      {:ok, cluster} = Cluster.start_link(masters: 3, replicas_per_master: 1, base_port: 7200)

      assert Cluster.all_alive?(cluster)
      assert Cluster.healthy?(cluster)

      info = Cluster.info(cluster)
      assert info.total_nodes == 6

      Cluster.stop(cluster)
      Process.sleep(1000)
    end
  end

  describe "Sentinel" do
    @tag timeout: 30_000
    test "start sentinel topology, verify health, stop" do
      {:ok, sentinel} =
        Sentinel.start_link(
          master_port: 6500,
          replicas: 2,
          sentinels: 3
        )

      assert wait_until(fn -> Sentinel.healthy?(sentinel) end)

      info = Sentinel.info(sentinel)
      assert info.master_name == "mymaster"
      assert info.replicas == 2
      assert info.sentinels == 3
      assert length(info.sentinel_addrs) == 3

      assert {:ok, master_info} = Sentinel.poke(sentinel)
      assert master_info["flags"] == "master"

      Sentinel.stop(sentinel)
      Process.sleep(1000)
    end
  end
end
