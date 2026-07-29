defmodule RedisServerWrapperTest do
  use ExUnit.Case, async: false

  alias RedisServerWrapper.{
    Cli,
    Cluster,
    Config,
    Connection,
    Manager,
    OSProcess,
    Sentinel,
    Server
  }

  setup_all do
    tls_dir =
      Path.join(
        System.tmp_dir!(),
        "redis-server-wrapper-tls-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tls_dir)
    tls = generate_tls_files!(tls_dir)
    on_exit(fn -> File.rm_rf!(tls_dir) end)
    {:ok, tls: tls}
  end

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

  defp redis_cli_wrapper!(dir, mode) do
    path = Path.join(dir, "redis-cli-#{mode}")

    body =
      case mode do
        :malformed_cluster_info ->
          """
          #!/bin/sh
          case " $* " in
            *" CLUSTER INFO "*) printf 'cluster_state:ok\\ncluster_slots_assigned:not-a-number\\n'; exit 0 ;;
            *) exec redis-cli "$@" ;;
          esac
          """

        :malformed_sentinel_master ->
          """
          #!/bin/sh
          case " $* " in
            *" SENTINEL MASTER "*) printf 'flags\\nmaster\\nnum-slaves\\nnot-a-number\\n'; exit 0 ;;
            *) exec redis-cli "$@" ;;
          esac
          """

        :malformed_replication_info ->
          """
          #!/bin/sh
          case " $* " in
            *" INFO replication "*) printf 'role:slave\\nmaster_link_status:down\\nmaster_port:bad\\n'; exit 0 ;;
            *) exec redis-cli "$@" ;;
          esac
          """
      end

    File.write!(path, body)
    File.chmod!(path, 0o700)
    path
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
      config =
        Config.new(
          extra: [
            {"notify-keyspace-events", "KEA"},
            {"client-output-buffer-limit", ["pubsub", "32mb", "8mb", "60"]},
            {"hz", "20"}
          ]
        )

      output = Config.to_config_string(config)
      assert output =~ "notify-keyspace-events KEA"
      assert output =~ "client-output-buffer-limit pubsub 32mb 8mb 60"
      assert output =~ "hz 20"
    end

    test "quotes and escapes every directive token safely" do
      config =
        Config.new(
          password: "space \"quote\" \\ slash\nnext#",
          logfile: "/tmp/redis logs/quoted\"name.log",
          replicaof: {"replica host", 6379},
          extra: [{"acl-pubsub-default", "resetchannels\nallchannels"}]
        )

      output = Config.to_config_string(config)

      assert output =~ ~s(requirepass "space \\"quote\\" \\\\ slash\\nnext#")
      assert output =~ ~s(logfile "/tmp/redis logs/quoted\\"name.log")
      assert output =~ ~s(replicaof "replica host" 6379)
      assert output =~ ~s(acl-pubsub-default "resetchannels\\nallchannels")
      refute output =~ "slash\nnext"
    end

    test "supports multiple bind addresses with a separate control host" do
      config = Config.new(bind: ["127.0.0.1", "::1"], control_host: "::1")

      assert Config.to_config_string(config) =~ "bind 127.0.0.1 ::1"
      assert Config.listen_addresses(config) == ["127.0.0.1", "::1"]
      assert Config.control_host(config) == "::1"
    end

    test "rejects reserved extras and invalid typed values" do
      assert_raise ArgumentError, ~r/cannot override.*port/, fn ->
        Config.new(extra: [{"port", "9999"}])
      end

      assert_raise ArgumentError, ~r/cannot override.*daemonize/, fn ->
        Config.new(extra: [{"daemonize", ["yes"]}])
      end

      assert_raise ArgumentError, ~r/:port must be/, fn -> Config.new(port: 70_000) end
      assert_raise ArgumentError, ~r/:loglevel must be/, fn -> Config.new(loglevel: :loud) end
      assert_raise ArgumentError, ~r/:appendonly must be/, fn -> Config.new(appendonly: "yes") end

      assert_raise ArgumentError, ~r/:save entries must be/, fn ->
        Config.new(save: [{0, 1}])
      end

      assert_raise ArgumentError, ~r/argument_vector/, fn ->
        Config.new(extra: [{"hz", ["10", :unsafe]}])
      end
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

      assert output =~ ~s(loadmodule "/modules/legacy.so events expired")

      assert output =~
               ~s(loadmodule "/modules/event stream.so" events expired,set maxlen 10000)
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
          tls_ca_cert_file: "/path/ca.pem",
          tls_auth_clients: "no"
        )

      output = Config.to_config_string(config)
      assert output =~ "tls-port 6380"
      assert output =~ "tls-cert-file /path/cert.pem"
      assert output =~ "tls-key-file /path/key.pem"
      assert output =~ "tls-ca-cert-file /path/ca.pem"
      assert output =~ "tls-auth-clients no"
    end

    test "named ACL auth and operational endpoint selection" do
      acl = Config.new(username: "app user", password: "secret value")
      output = Config.to_config_string(acl)

      assert output =~ "user default off"
      assert output =~ ~s(user "app user" on ">secret value" ~* &* +@all)
      refute output =~ "requirepass"

      assert %Connection{
               transport: :tcp,
               username: "app user",
               password: "secret value"
             } = Config.connection(acl)

      unix = Config.new(port: 0, unixsocket: "/tmp/redis wrapper.sock")

      assert %Connection{transport: :unix, socket: "/tmp/redis wrapper.sock"} =
               Config.connection(unix)

      assert_raise ArgumentError, ~r/:username requires/, fn ->
        Config.new(username: "app")
      end

      assert_raise ArgumentError, ~r/at least one/, fn ->
        Config.new(port: 0)
      end

      assert_raise ArgumentError, ~r/:unixsocket must be at most 103 bytes/, fn ->
        Config.new(port: 0, unixsocket: "/" <> String.duplicate("a", 104))
      end
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
      assert info.distribution == :core

      assert {:ok, "OK"} = Server.run(server, ["SET", "mykey", "myvalue"])
      assert {:ok, "myvalue"} = Server.run(server, ["GET", "mykey"])

      Server.stop(server)
      Process.sleep(1000)
    end

    test "start with password" do
      password = "test secret\"\\\n#"
      {:ok, server} = Server.start_link(port: 6401, password: password)

      assert Server.ping(server)
      assert {:ok, "OK"} = Server.run(server, ["SET", "k", "v"])
      assert {:ok, "v"} = Server.run(server, ["GET", "k"])

      node_dir = Server.info(server).node_dir
      assert file_mode(node_dir) == 0o700
      assert file_mode(Path.join(node_dir, "redis.conf")) == 0o600

      Server.stop(server)
      Process.sleep(500)
    end

    test "start over a Unix socket with TCP disabled" do
      socket =
        Path.join(
          System.tmp_dir!(),
          "redis-server-wrapper-#{System.unique_integer([:positive])}.sock"
        )

      {:ok, server} =
        Server.start_link(
          port: 0,
          unixsocket: socket,
          unixsocketperm: "700"
        )

      try do
        assert Server.ping(server)
        assert {:ok, "OK"} = Server.run(server, ["SET", "unix-key", "unix-value"])
        assert {:ok, "unix-value"} = Server.run(server, ["GET", "unix-key"])
        assert %Connection{transport: :unix, socket: ^socket} = Server.cli(server).connection
      after
        Server.stop(server)
      end

      refute File.exists?(socket)
    end

    test "start with named ACL credentials" do
      {:ok, server} =
        Server.start_link(
          port: 6407,
          username: "wrapper-app",
          password: "acl secret"
        )

      try do
        assert Server.ping(server)
        assert {:ok, "OK"} = Server.run(server, ["SET", "acl-key", "acl-value"])
        assert {:ok, "acl-value"} = Server.run(server, ["GET", "acl-key"])

        assert %Connection{username: "wrapper-app", password: "acl secret"} =
                 Server.cli(server).connection
      after
        Server.stop(server)
      end
    end

    test "start TLS-only with CA verification, SNI, and client authentication", %{tls: tls} do
      {:ok, server} =
        Server.start_link(
          port: 0,
          tls_port: 6408,
          tls_cert_file: tls.cert,
          tls_key_file: tls.key,
          tls_ca_cert_file: tls.ca,
          tls_auth_clients: "yes",
          tls_client_cert_file: tls.cert,
          tls_client_key_file: tls.key,
          tls_server_name: "localhost",
          username: "tls-app",
          password: "tls secret"
        )

      try do
        assert Server.ping(server)
        assert {:ok, "OK"} = Server.run(server, ["SET", "tls-key", "tls-value"])
        assert {:ok, "tls-value"} = Server.run(server, ["GET", "tls-key"])

        assert %Connection{
                 transport: :tls,
                 port: 6408,
                 username: "tls-app",
                 tls_server_name: "localhost"
               } = Server.cli(server).connection
      after
        Server.stop(server)
      end
    end

    test "multiple listen addresses use an explicit control host" do
      {:ok, server} =
        Server.start_link(
          port: 6405,
          bind: ["127.0.0.1", "::1"],
          control_host: "127.0.0.1"
        )

      try do
        assert Server.ping(server)
        assert Server.info(server).bind == ["127.0.0.1", "::1"]
        assert Server.info(server).host == "127.0.0.1"
        assert Server.cli(server).connection.host == "127.0.0.1"
      after
        Server.stop(server)
      end
    end

    test "loads the version-matrix module when the compatibility job provides it" do
      case System.get_env("REDIS_TEST_MODULE") do
        nil ->
          :ok

        module_path ->
          {:ok, server} = Server.start_link(port: 6406, loadmodule: [module_path])

          try do
            assert Server.ping(server)
            assert {:ok, modules} = Server.run(server, ["MODULE", "LIST"])
            assert modules =~ "wrapper_version_matrix"
          after
            Server.stop(server)
          end
      end
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

    test "managed Server exits when Redis dies and the endpoint can be reused" do
      port = 6416
      {:ok, stale_server} = Server.start(port: port)
      stale_pid = Server.info(stale_server).pid
      monitor = Process.monitor(stale_server)

      assert :ok = RedisServerWrapper.OSProcess.signal(stale_pid, :kill)

      assert_receive {:DOWN, ^monitor, :process, ^stale_server, {:redis_server_exit, :port, _}},
                     5_000

      {:ok, replacement_server} = Server.start_link(port: port)

      try do
        assert Server.ping(replacement_server)
      after
        if Process.alive?(replacement_server), do: Server.stop(replacement_server)
      end
    end

    test "an OTP supervisor restarts Server after the managed Redis process dies" do
      port = 6417
      {:ok, supervisor} = Supervisor.start_link([{Server, [port: port]}], strategy: :one_for_one)

      [{_id, original_server, :worker, _modules}] = Supervisor.which_children(supervisor)
      original_os_pid = Server.info(original_server).pid
      assert :ok = RedisServerWrapper.OSProcess.signal(original_os_pid, :kill)

      assert wait_until(
               fn ->
                 case Supervisor.which_children(supervisor) do
                   [{_id, restarted_server, :worker, _modules}]
                   when is_pid(restarted_server) and restarted_server != original_server ->
                     Server.ping(restarted_server)

                   _other ->
                     false
                 end
               end,
               50,
               100
             )

      Supervisor.stop(supervisor)
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
        Enum.each([6430, 6470, 6471, 6488, 6489, 7430, 7431, 7432, 26_470], fn port ->
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

    test "Unix-socket instances persist their connection and stop cleanly" do
      socket =
        Path.join(
          System.tmp_dir!(),
          "rsw-manager-#{System.unique_integer([:positive])}.sock"
        )

      assert {:ok, instance} =
               Manager.start_basic(
                 name: "unix-basic",
                 port: 0,
                 unixsocket: socket,
                 unixsocketperm: "700",
                 username: "manager-app",
                 password: "manager secret"
               )

      cli = Cli.new(connection: instance.connection)

      try do
        assert instance.ports == []
        assert instance.url == "unix://#{URI.encode(socket)}"
        assert instance.connection.transport == :unix
        assert Cli.ping(cli)

        assert {:ok, credentials} = Manager.credentials(instance.name)
        assert credentials.username == "manager-app"
        assert credentials.url == instance.url

        assert :ok = Manager.stop(instance.name)
        assert wait_until(fn -> not Cli.ping(cli) end, 5, 200)
        refute File.exists?(socket)
      after
        if Cli.ping(cli), do: Manager.stop(instance.name)
        File.rm(socket)
      end
    end

    test "credentials stay out of default output and state is private", %{state_file: state_file} do
      password = "a:b@/?#%"
      parent = self()

      output =
        ExUnit.CaptureIO.capture_io(fn ->
          send(
            parent,
            {:started,
             Manager.start_basic(
               name: "redacted-basic",
               port: 6488,
               password: password
             )}
          )
        end)

      assert_receive {:started, {:ok, instance}}

      try do
        refute output =~ password
        assert output =~ "[REDACTED]"
        assert file_mode(Path.dirname(state_file)) == 0o700
        assert file_mode(state_file) == 0o600

        assert {:ok, credentials} = Manager.credentials(instance.name)
        assert credentials.password == password

        assert credentials.url ==
                 "redis://:a%3Ab%40%2F%3F%23%25@127.0.0.1:6488"

        info_output =
          ExUnit.CaptureIO.capture_io(fn ->
            assert {:ok, %{name: "redacted-basic"}} = Manager.info(instance.name)
          end)

        refute info_output =~ password
        assert info_output =~ "[REDACTED]"
      after
        Manager.stop(instance.name)
      end
    end

    test "credentials encode IPv6 hosts and URL-significant passwords", %{state_file: state_file} do
      password = "space and:@/%?#"

      write_manager_state(state_file, %{
        "ipv6-basic" =>
          manager_instance("ipv6-basic", "basic", "2026-01-01T00:00:00Z")
          |> Map.put("bind", "::1")
          |> Map.put("ports", [6379])
          |> Map.put("password", password)
      })

      assert {:ok, %{password: ^password, url: url}} = Manager.credentials("ipv6-basic")
      assert url == "redis://:space%20and%3A%40%2F%25%3F%23@[::1]:6379"
    end

    test "invalid state is preserved and does not crash Manager", %{state_file: state_file} do
      File.mkdir_p!(Path.dirname(state_file))
      File.write!(state_file, ~s({"instances":))
      File.chmod!(state_file, 0o644)

      assert Manager.list() == []
      refute File.exists?(state_file)

      assert [backup] = Path.wildcard("#{state_file}.corrupt-*")
      assert File.read!(backup) == ~s({"instances":)
      assert file_mode(backup) == 0o600
    end

    test "unknown metadata keys never create atoms", %{state_file: state_file} do
      unknown_key = "manager_unknown_#{System.unique_integer([:positive])}"

      assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_key) end

      write_manager_state(state_file, %{
        "metadata-basic" =>
          manager_instance("metadata-basic", "basic", "2026-01-01T00:00:00Z")
          |> Map.put("metadata", %{unknown_key => "retained"})
      })

      assert {:ok, info} = Manager.info("metadata-basic")
      assert info.metadata[unknown_key] == "retained"
      assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_key) end
    end

    @tag timeout: 30_000
    test "concurrent Manager mutations retain every instance and valid JSON", %{
      state_file: state_file
    } do
      tasks =
        [
          {"concurrent-a", 6488},
          {"concurrent-b", 6489}
        ]
        |> Enum.map(fn {name, port} ->
          Task.async(fn -> Manager.start_basic(name: name, port: port, password: nil) end)
        end)

      assert Enum.all?(Task.await_many(tasks, 20_000), &match?({:ok, _instance}, &1))

      assert Manager.list()
             |> Enum.map(& &1.name)
             |> Enum.sort() == ["concurrent-a", "concurrent-b"]

      assert {:ok, %{"instances" => instances}} =
               state_file |> File.read!() |> JSON.decode()

      assert Map.keys(instances) |> Enum.sort() == ["concurrent-a", "concurrent-b"]
      assert file_mode(state_file) == 0o600
      assert :ok = Manager.stop_all()
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

    test "stale Manager state never shuts down a foreign listener", %{state_file: state_file} do
      port = 6485
      {:ok, existing_server} = Server.start_link(port: port, save: :disabled)

      write_manager_state(
        state_file,
        %{
          "stale-basic" =>
            manager_instance("stale-basic", "basic", "2026-01-01T00:00:00Z")
            |> Map.put("ports", [port])
        }
      )

      try do
        assert :ok = Manager.stop("stale-basic")
        assert Server.ping(existing_server)
        assert Manager.list() == []
      after
        if Process.alive?(existing_server), do: Server.stop(existing_server)
      end
    end

    test "Manager retains state when a live recorded PID cannot be tied to a listener", %{
      state_file: state_file
    } do
      live_pid = System.pid() |> String.to_integer()

      write_manager_state(
        state_file,
        %{
          "unverified-basic" =>
            manager_instance("unverified-basic", "basic", "2026-01-01T00:00:00Z")
            |> Map.put("pids", [live_pid])
        }
      )

      assert {:error, {:process_ownership_not_verified, "unverified-basic", [^live_pid]}} =
               Manager.stop("unverified-basic")

      assert [%{name: "unverified-basic"}] = Manager.list()

      assert {:error,
              {:instances_not_stopped,
               %{
                 "unverified-basic" =>
                   {:process_ownership_not_verified, "unverified-basic", [^live_pid]}
               }}} = Manager.stop_all()

      assert [%{name: "unverified-basic"}] = Manager.list()
    end

    test "Manager refuses teardown when listener ownership cannot be inspected", %{
      state_file: state_file
    } do
      write_manager_state(
        state_file,
        %{
          "uninspectable-basic" =>
            manager_instance("uninspectable-basic", "basic", "2026-01-01T00:00:00Z")
            |> Map.put("ports", [6486])
        }
      )

      original_path = System.get_env("PATH")
      empty_path = Path.join(Path.dirname(state_file), "empty-path")
      File.mkdir_p!(empty_path)
      System.put_env("PATH", empty_path)

      try do
        assert {:error,
                {:process_ownership_not_verified, "uninspectable-basic",
                 {:executable_not_found, "lsof"}}} = Manager.stop("uninspectable-basic")
      after
        System.put_env("PATH", original_path)
      end

      assert [%{name: "uninspectable-basic"}] = Manager.list()
    end

    test "Manager signals only the recorded listener when graceful authentication fails", %{
      state_file: state_file
    } do
      port = 6487

      {:ok, server} =
        Server.start_link(
          port: port,
          password: "actual-password",
          managed: false,
          save: :disabled
        )

      server_info = Server.info(server)

      write_manager_state(
        state_file,
        %{
          "owned-basic" =>
            manager_instance("owned-basic", "basic", "2026-01-01T00:00:00Z")
            |> Map.put("ports", [port])
            |> Map.put("pids", [server_info.pid])
            |> Map.put("password", "wrong-password")
        }
      )

      try do
        assert :ok = Manager.stop("owned-basic")
        assert wait_until(fn -> not Server.alive?(server) end, 10, 200)
      after
        if Process.alive?(server), do: Server.stop(server)
      end
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
    test "occupied node ports fail closed without stopping the existing server or deleting data" do
      port = 7460
      {:ok, existing_server} = Server.start_link(port: port, save: :disabled)
      existing_info = Server.info(existing_server)
      unrelated_dump = Path.join(existing_info.node_dir, "dump.rdb")
      File.write!(unrelated_dump, "belongs to the existing server")

      try do
        assert {:error, {:node_start_failed, ^port, {:port_in_use, ^port, _reason}}} =
                 Cluster.start(masters: 1, base_port: port)

        assert Server.ping(existing_server)
        assert File.read!(unrelated_dump) == "belongs to the existing server"
      after
        if Process.alive?(existing_server), do: Server.stop(existing_server)
      end
    end

    test "partial node startup rolls back nodes already started" do
      base_port = 7465
      occupied_port = base_port + 1
      {:ok, existing_server} = Server.start_link(port: occupied_port, save: :disabled)

      try do
        assert {:error,
                {:node_start_failed, ^occupied_port, {:port_in_use, ^occupied_port, _reason}}} =
                 Cluster.start(masters: 3, base_port: base_port)

        refute Cli.ping(Cli.new(port: base_port))
        assert Server.ping(existing_server)
      after
        if Process.alive?(existing_server), do: Server.stop(existing_server)
      end
    end

    @tag timeout: 30_000
    test "a hard-dead cluster node leaves an explicit degraded topology" do
      {:ok, cluster} = Cluster.start_link(masters: 3, base_port: 7480)
      assert Cluster.healthy?(cluster)

      [_first, victim | _rest] = Cluster.nodes(cluster)
      victim_os_pid = Server.info(victim).pid
      assert :ok = RedisServerWrapper.OSProcess.signal(victim_os_pid, :kill)

      assert wait_until(fn -> not Process.alive?(victim) end, 50, 100)
      assert Process.alive?(cluster)
      refute Cluster.all_alive?(cluster)
      refute Cluster.healthy?(cluster)
      assert length(Cluster.nodes(cluster)) == 2

      Cluster.stop(cluster)
    end

    @tag timeout: 30_000
    test "cluster convergence timeout is actionable and rolls back every node" do
      temp_dir =
        Path.join(
          System.tmp_dir!(),
          "redis-server-wrapper-cli-#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(temp_dir)
      cli = redis_cli_wrapper!(temp_dir, :malformed_cluster_info)

      try do
        assert {:error, {:cluster_convergence_timeout, 200, _last_health}} =
                 Cluster.start(
                   masters: 3,
                   base_port: 7490,
                   redis_cli_bin: cli,
                   convergence_timeout: 200
                 )

        assert Enum.all?(7490..7492, &(not Cli.ping(Cli.new(port: &1))))
      after
        File.rm_rf!(temp_dir)
      end
    end

    @tag timeout: 30_000
    test "start 3-master cluster, verify health, stop" do
      {:ok, cluster} =
        RedisServerWrapper.start_cluster(
          masters: 3,
          base_port: 7100,
          bind: ["127.0.0.1", "::1"],
          control_host: "127.0.0.1"
        )

      assert Cluster.all_alive?(cluster)
      assert wait_until(fn -> Cluster.healthy?(cluster) end)
      assert Cluster.addr(cluster) == "127.0.0.1:7100"
      assert Cluster.info(cluster).bind == ["127.0.0.1", "::1"]

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

    @tag timeout: 30_000
    test "TLS-only cluster forms and serves commands", %{tls: tls} do
      {:ok, cluster} =
        Cluster.start_link(
          masters: 3,
          base_port: 7120,
          tls: true,
          tls_cert_file: tls.cert,
          tls_key_file: tls.key,
          tls_ca_cert_file: tls.ca,
          tls_auth_clients: "no",
          tls_server_name: "localhost",
          username: "cluster-app",
          password: "cluster secret"
        )

      try do
        assert Cluster.all_alive?(cluster)
        assert wait_until(fn -> Cluster.healthy?(cluster) end)
        assert {:ok, "PONG"} = Cluster.run(cluster, ["PING"])
        assert Cluster.info(cluster).connection.transport == :tls
      after
        Cluster.stop(cluster)
      end
    end
  end

  describe "Sentinel" do
    test "occupied master port fails closed without stopping the existing server" do
      master_port = 7470
      {:ok, existing_server} = Server.start_link(port: master_port, save: :disabled)

      try do
        assert {:error, {:port_in_use, ^master_port, _reason}} =
                 Sentinel.start(
                   master_port: master_port,
                   replicas: 0,
                   sentinels: 1,
                   sentinel_base_port: 27_470,
                   quorum: 1
                 )

        assert Server.ping(existing_server)
      after
        if Process.alive?(existing_server), do: Server.stop(existing_server)
      end
    end

    @tag timeout: 30_000
    test "a supervisor restarts Sentinel after a hard-dead control process" do
      child =
        {Sentinel,
         [
           master_port: 6530,
           replicas: 0,
           sentinels: 1,
           sentinel_base_port: 26_530,
           quorum: 1
         ]}

      {:ok, supervisor} = Supervisor.start_link([child], strategy: :one_for_one)
      [{_id, sentinel, :worker, _modules}] = Supervisor.which_children(supervisor)

      monitor = Process.monitor(sentinel)
      [control] = :sys.get_state(sentinel).sentinel_processes
      assert is_pid(control.owner)
      assert is_integer(control.os_pid)
      assert :ok = RedisServerWrapper.OSProcess.signal(control.os_pid, :kill)

      assert_receive {:DOWN, ^monitor, :process, ^sentinel,
                      {:sentinel_control_exit, 26_530, _reason}},
                     5_000

      assert wait_until(
               fn ->
                 case Supervisor.which_children(supervisor) do
                   [{_id, restarted, :worker, _modules}]
                   when is_pid(restarted) and restarted != sentinel ->
                     Sentinel.healthy?(restarted)

                   _other ->
                     false
                 end
               end,
               50,
               100
             )

      Supervisor.stop(supervisor)
    end

    @tag timeout: 30_000
    test "a hard-dead Sentinel data node fails and cleans up the topology" do
      {:ok, sentinel} =
        Sentinel.start(
          master_port: 6535,
          replicas: 1,
          replica_base_port: 6536,
          sentinels: 1,
          sentinel_base_port: 26_535,
          quorum: 1
        )

      monitor = Process.monitor(sentinel)
      [replica] = :sys.get_state(sentinel).replica_pids
      replica_os_pid = Server.info(replica).pid
      assert :ok = RedisServerWrapper.OSProcess.signal(replica_os_pid, :kill)

      assert_receive {:DOWN, ^monitor, :process, ^sentinel,
                      {:sentinel_data_node_exit, :replica, 6536, _reason}},
                     5_000

      assert wait_until(
               fn ->
                 Enum.all?([6535, 6536, 26_535], &(not Cli.ping(Cli.new(port: &1))))
               end,
               30,
               100
             )
    end

    @tag timeout: 30_000
    test "a hard-dead Sentinel master fails the topology coherently" do
      {:ok, sentinel} =
        Sentinel.start(
          master_port: 6565,
          replicas: 0,
          sentinels: 1,
          sentinel_base_port: 26_565,
          quorum: 1
        )

      monitor = Process.monitor(sentinel)
      master = :sys.get_state(sentinel).master_pid
      master_os_pid = Server.info(master).pid
      assert :ok = OSProcess.signal(master_os_pid, :kill)

      assert_receive {:DOWN, ^monitor, :process, ^sentinel,
                      {:sentinel_data_node_exit, :master, 6565, _reason}},
                     5_000

      assert wait_until(
               fn ->
                 not Cli.ping(Cli.new(port: 6565)) and
                   not Cli.ping(Cli.new(port: 26_565))
               end,
               30,
               100
             )
    end

    @tag timeout: 30_000
    test "Sentinel control processes honor the Forcola backend" do
      {:ok, sentinel} =
        Sentinel.start_link(
          master_port: 6538,
          replicas: 0,
          sentinels: 1,
          sentinel_base_port: 26_538,
          quorum: 1,
          managed: :forcola
        )

      state = :sys.get_state(sentinel)
      [control] = state.sentinel_processes
      assert control.managed == :forcola
      assert Process.alive?(control.owner)
      assert Sentinel.healthy?(sentinel)

      Sentinel.stop(sentinel)
      assert wait_until(fn -> not Cli.ping(Cli.new(port: 26_538)) end, 20, 100)
    end

    @tag timeout: 30_000
    test "managed false daemonizes Sentinel controls and normal stop reaps them" do
      {:ok, sentinel} =
        Sentinel.start_link(
          master_port: 6545,
          replicas: 0,
          sentinels: 1,
          sentinel_base_port: 26_545,
          quorum: 1,
          managed: false
        )

      [control] = :sys.get_state(sentinel).sentinel_processes
      assert control.managed == false
      assert is_nil(control.owner)
      assert OSProcess.alive?(control.os_pid)

      Sentinel.stop(sentinel)

      assert wait_until(
               fn ->
                 not OSProcess.alive?(control.os_pid) and
                   not Cli.ping(Cli.new(port: 6545))
               end,
               30,
               100
             )
    end

    @tag timeout: 30_000
    test "replication convergence timeout is actionable and rolls back data nodes" do
      temp_dir =
        Path.join(
          System.tmp_dir!(),
          "redis-server-wrapper-cli-#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(temp_dir)
      cli = redis_cli_wrapper!(temp_dir, :malformed_replication_info)

      try do
        assert {:error, {:replication_convergence_timeout, 200, _last_health}} =
                 Sentinel.start(
                   master_port: 6550,
                   replicas: 1,
                   replica_base_port: 6551,
                   sentinels: 1,
                   sentinel_base_port: 26_550,
                   quorum: 1,
                   redis_cli_bin: cli,
                   convergence_timeout: 200
                 )

        assert Enum.all?([6550, 6551, 26_550], &(not Cli.ping(Cli.new(port: &1))))
      after
        File.rm_rf!(temp_dir)
      end
    end

    @tag timeout: 30_000
    test "Sentinel control startup failure rolls back its data node" do
      fixture_dir =
        Path.join(
          System.tmp_dir!(),
          "redis-server-wrapper-server-#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(fixture_dir)

      redis_server =
        Path.join(fixture_dir, "redis-server-fail-sentinel")

      File.write!(
        redis_server,
        """
        #!/bin/sh
        for arg in "$@"; do
          if [ "$arg" = "--sentinel" ]; then
            exit 42
          fi
        done
        exec redis-server "$@"
        """
      )

      File.chmod!(redis_server, 0o700)

      try do
        assert {:error, {:sentinel_start_failed, 26_560, _reason}} =
                 Sentinel.start(
                   master_port: 6560,
                   replicas: 0,
                   sentinels: 1,
                   sentinel_base_port: 26_560,
                   quorum: 1,
                   redis_server_bin: redis_server
                 )

        assert wait_until(fn -> not Cli.ping(Cli.new(port: 6560)) end, 20, 100)
      after
        File.rm_rf!(fixture_dir)
      end
    end

    @tag timeout: 30_000
    test "Sentinel convergence timeout handles malformed health and rolls back" do
      temp_dir =
        Path.join(
          System.tmp_dir!(),
          "redis-server-wrapper-cli-#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(temp_dir)
      cli = redis_cli_wrapper!(temp_dir, :malformed_sentinel_master)

      try do
        assert {:error, {:sentinel_convergence_timeout, 200, _last_health}} =
                 Sentinel.start(
                   master_port: 6540,
                   replicas: 0,
                   sentinels: 1,
                   sentinel_base_port: 26_540,
                   quorum: 1,
                   redis_cli_bin: cli,
                   convergence_timeout: 200
                 )

        assert Enum.all?([6540, 26_540], &(not Cli.ping(Cli.new(port: &1))))
      after
        File.rm_rf!(temp_dir)
      end
    end

    @tag timeout: 30_000
    test "sentinel topology supports zero replicas without creating phantom ports" do
      {:ok, sentinel} =
        Sentinel.start_link(
          master_port: 6510,
          replicas: 0,
          sentinels: 1,
          sentinel_base_port: 26_510,
          quorum: 1,
          bind: ["127.0.0.1", "::1"],
          control_host: "127.0.0.1",
          password: "sentinel secret\"\\\n#"
        )

      try do
        assert wait_until(fn -> Sentinel.healthy?(sentinel) end)
        assert Sentinel.info(sentinel).replicas == 0
        assert Sentinel.sentinel_addrs(sentinel) == ["127.0.0.1:26510"]

        sentinel_dir = :sys.get_state(sentinel).sentinel_dir
        sentinel_conf = Path.join([sentinel_dir, "sentinel-26510", "sentinel.conf"])
        assert file_mode(sentinel_dir) == 0o700
        assert file_mode(Path.dirname(sentinel_conf)) == 0o700
        assert file_mode(sentinel_conf) == 0o600
      after
        if Process.alive?(sentinel), do: Sentinel.stop(sentinel)
      end
    end

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

    @tag timeout: 30_000
    test "TLS-only Sentinel uses named ACL auth for monitoring and replication", %{tls: tls} do
      {:ok, sentinel} =
        Sentinel.start_link(
          master_port: 6520,
          replicas: 1,
          replica_base_port: 6521,
          sentinels: 1,
          sentinel_base_port: 26_520,
          quorum: 1,
          tls: true,
          tls_cert_file: tls.cert,
          tls_key_file: tls.key,
          tls_ca_cert_file: tls.ca,
          tls_auth_clients: "no",
          tls_server_name: "localhost",
          username: "sentinel-app",
          password: "sentinel secret"
        )

      try do
        assert wait_until(fn -> Sentinel.healthy?(sentinel) end)
        assert {:ok, master_info} = Sentinel.poke(sentinel)
        assert master_info["flags"] == "master"
        assert Sentinel.info(sentinel).master_connection.transport == :tls
      after
        Sentinel.stop(sentinel)
      end
    end
  end

  defp generate_tls_files!(dir) do
    openssl =
      System.find_executable("openssl") ||
        raise "openssl is required for Redis TLS integration tests"

    ca_key = Path.join(dir, "ca.key")
    ca = Path.join(dir, "ca.crt")
    key = Path.join(dir, "server.key")
    csr = Path.join(dir, "server.csr")
    cert = Path.join(dir, "server.crt")
    extensions = Path.join(dir, "server.ext")

    openssl!(
      openssl,
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-sha256",
        "-days",
        "2",
        "-subj",
        "/CN=RedisServerWrapper Test CA",
        "-keyout",
        ca_key,
        "-out",
        ca
      ]
    )

    openssl!(
      openssl,
      [
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-sha256",
        "-subj",
        "/CN=localhost",
        "-keyout",
        key,
        "-out",
        csr
      ]
    )

    File.write!(
      extensions,
      "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n"
    )

    openssl!(
      openssl,
      [
        "x509",
        "-req",
        "-sha256",
        "-days",
        "2",
        "-in",
        csr,
        "-CA",
        ca,
        "-CAkey",
        ca_key,
        "-CAcreateserial",
        "-extfile",
        extensions,
        "-out",
        cert
      ]
    )

    %{ca: ca, cert: cert, key: key}
  end

  defp openssl!(openssl, args) do
    case System.cmd(openssl, args, stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> raise "openssl failed (#{status}): #{output}"
    end
  end

  defp write_manager_state(path, instances, counters \\ %{}) do
    state = %{"instances" => instances, "counters" => counters}
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, JSON.encode!(state))
  end

  defp file_mode(path), do: Bitwise.band(File.stat!(path).mode, 0o777)

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
