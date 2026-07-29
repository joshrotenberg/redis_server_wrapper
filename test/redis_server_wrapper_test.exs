defmodule RedisServerWrapperTest do
  use ExUnit.Case, async: false

  alias RedisServerWrapper.{Cli, Cluster, Config, Manager, Sentinel, Server}

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

  # Returns the list of PIDs listening on the given TCP port, or [] if none.
  defp lsof_port(port) do
    case System.cmd("lsof", ["-ti", ":#{port}"], stderr_to_stdout: true) do
      {out, 0} -> out |> String.split("\n", trim: true)
      _ -> []
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

    test "module paths and structured module arguments" do
      config =
        Config.new(
          loadmodule: [
            "/modules/legacy.so events expired",
            {"/modules/event stream.so", ["events", "expired,set", "maxlen", "10000"]}
          ]
        )

      output = Config.to_config_string(config)

      assert output =~ "loadmodule /modules/legacy.so events expired"

      assert output =~
               ~s(loadmodule "/modules/event stream.so" "events" "expired,set" "maxlen" "10000")
    end

    test "rejects invalid module specifications" do
      assert_raise ArgumentError, ~r/:loadmodule must be a list/, fn ->
        Config.new(loadmodule: [{"/modules/bad.so", [:not_a_string]}])
      end
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
      {:ok, server} = RedisServerWrapper.start_server(port: 6400)

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

    test "managed server can restart immediately after a client connection" do
      {:ok, server} = Server.start_link(port: 6414)

      assert {:ok, "OK"} = Server.run(server, ["SET", "restart_key", "restart_val"])
      assert :ok = Server.stop(server)

      assert {:ok, restarted} = Server.start_link(port: 6414)
      assert Server.ping(restarted)
      assert :ok = Server.stop(restarted)
    end

    test "managed server still rejects a port with a live listener" do
      {:ok, server} = Server.start_link(port: 6415)

      assert {:error, {:port_in_use, 6415, _reason}} =
               Server.start(port: 6415)

      assert :ok = Server.stop(server)
    end
  end

  describe "Server forcola mode" do
    test "forcola server starts, pings, and runs commands" do
      {:ok, server} = Server.start_link(port: 6420, managed: :forcola)

      assert Server.ping(server)
      assert Server.alive?(server)

      info = Server.info(server)
      assert info.managed == :forcola
      assert info.port == 6420
      assert info.pid != nil

      assert {:ok, "OK"} = Server.run(server, ["SET", "forcola_key", "forcola_val"])
      assert {:ok, "forcola_val"} = Server.run(server, ["GET", "forcola_key"])

      Server.stop(server)
      Process.sleep(1000)

      assert lsof_port(6420) == []
    end

    test "forcola server reaps redis-server when GenServer stops" do
      {:ok, server} = Server.start_link(port: 6421, managed: :forcola)
      assert Server.ping(server)
      assert lsof_port(6421) != []

      Server.stop(server)
      Process.sleep(1000)

      assert lsof_port(6421) == []
    end

    test "forcola server cannot detach from the BEAM lifecycle" do
      {:ok, server} = Server.start_link(port: 6423, managed: :forcola)

      assert {:error, :managed_server} = Server.detach(server)
      refute Server.info(server).detached

      Server.stop(server)
    end

    # The core guarantee: terminate/2 does NOT run on a :brutal_kill, but the
    # forcola shim still kills the redis-server process group on owner death,
    # so the port is released. This is the leak the Port path cannot close.
    test "forcola server reaps redis-server on a brutal kill (terminate never runs)" do
      Process.flag(:trap_exit, true)
      {:ok, server} = Server.start_link(port: 6422, managed: :forcola)
      assert Server.ping(server)
      assert lsof_port(6422) != []

      Process.exit(server, :kill)

      assert wait_until(fn -> lsof_port(6422) == [] end, 10, 500)
    end

    test "invalid managed value yields a clear error" do
      assert {:error, {:invalid_managed, :bogus}} = Server.start(port: 6424, managed: :bogus)
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

  describe "Manager" do
    setup do
      temp_dir =
        Path.join([
          System.tmp_dir!(),
          "redis-server-wrapper-tests",
          "manager-#{System.unique_integer([:positive])}"
        ])

      state_file = Path.join(temp_dir, "instances.json")
      previous_state_file = Application.fetch_env(:redis_server_wrapper, :manager_state_file)
      Application.put_env(:redis_server_wrapper, :manager_state_file, state_file)

      on_exit(fn ->
        Enum.each([6430, 6470, 6471, 7430, 7431, 7432, 26_470], fn port ->
          Cli.shutdown(Cli.new(port: port))
        end)

        case previous_state_file do
          {:ok, value} ->
            Application.put_env(:redis_server_wrapper, :manager_state_file, value)

          :error ->
            Application.delete_env(:redis_server_wrapper, :manager_state_file)
        end

        File.rm_rf!(temp_dir)
      end)

      {:ok, state_file: state_file}
    end

    test "basic instances remain running after the launcher GenServer exits" do
      assert {:ok, instance} = Manager.start_basic(port: 6430)
      on_exit(fn -> Manager.stop(instance.name) end)

      assert [_ | _] = instance.pids
      assert instance.name == "redis-basic-1"
      assert is_binary(instance.password)
      assert String.length(instance.password) == 16
      assert instance.url =~ instance.password

      cli = Cli.new(port: 6430, password: instance.password)
      assert wait_until(fn -> Cli.ping(cli) end, 5, 200)
      assert {:ok, %{status: :running}} = Manager.info(instance.name)

      assert {:error, {:instance_exists, "redis-basic-1"}} =
               Manager.start_basic(name: instance.name, port: 6430)

      assert :ok = Manager.stop(instance.name)
      assert wait_until(fn -> not Cli.ping(cli) end, 5, 200)
    end

    test "persisted state supports listing, filtering, details, and missing names", %{
      state_file: state_file
    } do
      write_manager_state(state_file, %{
        "dead-basic" => manager_instance("dead-basic", "basic", "2026-01-01T00:00:00Z"),
        "dead-cluster" => manager_instance("dead-cluster", "cluster", "2026-01-02T00:00:00Z")
      })

      assert Enum.map(Manager.list(), & &1.name) == ["dead-basic", "dead-cluster"]
      assert Enum.map(Manager.list(:cluster), & &1.name) == ["dead-cluster"]

      assert {:ok, info} = Manager.info("dead-basic")
      assert info.status == :stopped
      assert info.metadata == %{nested: %{enabled: true}, items: [%{value: 1}]}

      assert {:error, :not_found} = Manager.info("missing")
      assert {:error, :not_found} = Manager.stop("missing")
    end

    test "cleanup removes dead persisted instances", %{state_file: state_file} do
      write_manager_state(state_file, %{
        "dead-basic" => manager_instance("dead-basic", "basic", "2026-01-01T00:00:00Z"),
        "dead-cluster" => manager_instance("dead-cluster", "cluster", "2026-01-02T00:00:00Z")
      })

      assert Manager.cleanup() == {0, 2}
      assert Manager.list() == []
    end

    test "stop_all clears persisted instances and counters", %{state_file: state_file} do
      write_manager_state(
        state_file,
        %{"dead-basic" => manager_instance("dead-basic", "basic", "2026-01-01T00:00:00Z")},
        %{"basic" => 4}
      )

      assert :ok = Manager.stop_all()
      assert Manager.list() == []
    end

    test "topology launchers preserve startup errors" do
      invalid_modules = [{"/missing/module.so", [:not_a_string]}]

      assert {:error, _reason} =
               Manager.start_basic(
                 name: "invalid-basic",
                 port: 6442,
                 password: nil,
                 loadmodule: invalid_modules
               )

      assert {:error, _reason} =
               Manager.start_cluster(
                 name: "invalid-cluster",
                 base_port: 7440,
                 password: nil,
                 loadmodule: invalid_modules
               )

      assert {:error, _reason} =
               Manager.start_sentinel(
                 name: "invalid-sentinel",
                 master_port: 6480,
                 password: nil,
                 loadmodule: invalid_modules
               )
    end

    @tag timeout: 30_000
    test "cluster instances persist without orphaning node GenServers" do
      assert {:ok, _instance} =
               Manager.start_cluster(
                 name: "demo-cluster",
                 masters: 3,
                 base_port: 7430,
                 password: nil
               )

      assert wait_until(
               fn -> Enum.all?(7430..7432, &Cli.ping(Cli.new(port: &1))) end,
               10,
               500
             )

      assert :ok = Manager.stop("demo-cluster")

      assert wait_until(
               fn -> Enum.all?(7430..7432, &(not Cli.ping(Cli.new(port: &1)))) end,
               10,
               200
             )
    end

    @tag timeout: 30_000
    test "sentinel instances persist after the launcher exits" do
      assert {:ok, _instance} =
               Manager.start_sentinel(
                 name: "demo-sentinel",
                 master_port: 6470,
                 replicas: 1,
                 sentinels: 1,
                 sentinel_base_port: 26_470,
                 password: nil
               )

      assert wait_until(
               fn ->
                 Enum.all?([6470, 6471, 26_470], &Cli.ping(Cli.new(port: &1)))
               end,
               10,
               500
             )

      assert :ok = Manager.stop("demo-sentinel")

      assert wait_until(
               fn ->
                 Enum.all?([6470, 6471, 26_470], &(not Cli.ping(Cli.new(port: &1))))
               end,
               10,
               200
             )
    end
  end

  describe "Cluster" do
    @tag timeout: 30_000
    test "start 3-master cluster, verify health, stop" do
      {:ok, cluster} = RedisServerWrapper.start_cluster(masters: 3, base_port: 7100)

      assert Cluster.all_alive?(cluster)
      assert wait_until(fn -> Cluster.healthy?(cluster) end)

      info = Cluster.info(cluster)
      assert info.masters == 3
      assert info.total_nodes == 3
      assert length(info.node_addrs) == 3
      assert Cluster.node_addrs(cluster) == info.node_addrs

      addr = Cluster.addr(cluster)
      assert addr == "127.0.0.1:7100"
      assert {:error, :managed_server} = Cluster.detach(cluster)

      Cluster.stop(cluster)
      Process.sleep(1000)
    end

    @tag timeout: 30_000
    test "cluster with replicas" do
      {:ok, cluster} = Cluster.start_link(masters: 3, replicas_per_master: 1, base_port: 7200)

      assert Cluster.all_alive?(cluster)
      assert wait_until(fn -> Cluster.healthy?(cluster) end)

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
        RedisServerWrapper.start_sentinel(
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
      assert Sentinel.master_addr(sentinel) == "127.0.0.1:6500"

      assert Sentinel.sentinel_addrs(sentinel) == [
               "127.0.0.1:26389",
               "127.0.0.1:26390",
               "127.0.0.1:26391"
             ]

      assert {:ok, master_info} = Sentinel.poke(sentinel)
      assert master_info["flags"] == "master"
      assert {:error, :managed_server} = Sentinel.detach(sentinel)

      Sentinel.stop(sentinel)
      Process.sleep(1000)
    end
  end

  defp write_manager_state(path, instances, counters \\ %{}) do
    state = %{"instances" => instances, "counters" => counters}
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, JSON.encode!(state))
  end

  defp manager_instance(name, type, created_at) do
    %{
      "name" => name,
      "type" => type,
      "created_at" => created_at,
      "bind" => "127.0.0.1",
      "ports" => [],
      "pids" => [99_999_999],
      "password" => "secret",
      "url" => "redis://:secret@127.0.0.1:6379",
      "metadata" => %{
        "nested" => %{"enabled" => true},
        "items" => [%{"value" => 1}]
      }
    }
  end
end
