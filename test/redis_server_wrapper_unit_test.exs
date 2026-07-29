defmodule RedisServerWrapperUnitTest do
  use ExUnit.Case, async: false

  alias RedisServerWrapper.{Cli, Cluster, OSProcess, SecureFile, Sentinel, Server}

  setup do
    fixture_dir =
      Path.join([
        System.tmp_dir!(),
        "redis-server-wrapper-tests",
        "unit-#{System.unique_integer([:positive])}"
      ])

    File.mkdir_p!(fixture_dir)

    cli_bin =
      write_executable(
        fixture_dir,
        "fake-redis-cli",
        ~S"""
        #!/bin/sh
        args="$*"

        case "$args" in
          *"--cluster create"*)
            case "$args" in
              *"failnode"*) echo "cluster-create-error"; exit 2 ;;
              *) echo "cluster-created" ;;
            esac
            ;;
          *"CLUSTER INFO"*)
            case "$args" in
              *"-h error "*) echo "cluster-info-error"; exit 2 ;;
              *) printf '# Stats\ncluster_state:ok\norphan\n' ;;
            esac
            ;;
          *"SENTINEL MASTER"*)
            case "$args" in
              *"-h error "*) echo "sentinel-error"; exit 2 ;;
              *) printf 'name\nmymaster\norphan\n' ;;
            esac
            ;;
          *"PING"*)
            case "$args" in
              *"-h ready "*) echo "PONG" ;;
              *"-h loading "*) echo "LOADING dataset" ;;
              *"-h busy "*) echo "BUSY running" ;;
              *"-h unexpected "*) echo "NOPE" ;;
              *) echo "connection-refused"; exit 2 ;;
            esac
            ;;
          *"SHOW_AUTH"*) printf 'auth=%s args=%s\n' "$REDISCLI_AUTH" "$args" ;;
          *"FAIL"*) echo "failure"; exit 2 ;;
          *) echo "$args" ;;
        esac
        """
      )

    version_bin =
      write_executable(fixture_dir, "versioned-server", "#!/bin/sh\necho 'redis v=9.8.7'\n")

    plain_version_bin =
      write_executable(fixture_dir, "plain-server", "#!/bin/sh\necho 'custom redis build'\n")

    failing_version_bin =
      write_executable(
        fixture_dir,
        "failing-server",
        "#!/bin/sh\necho 'version failed'; exit 2\n"
      )

    on_exit(fn -> File.rm_rf!(fixture_dir) end)

    {:ok,
     fixture_dir: fixture_dir,
     cli_bin: cli_bin,
     version_bin: version_bin,
     plain_version_bin: plain_version_bin,
     failing_version_bin: failing_version_bin}
  end

  test "CLI runs commands, assembles connection options, and raises on errors", %{cli_bin: bin} do
    assert %Cli{host: "127.0.0.1", port: 6379} = Cli.new()

    cli = Cli.new(bin: bin, host: "ready", port: 6380, password: "secret", tls: true)

    assert {:ok, args} = Cli.run(cli, ["ECHO"])
    assert args =~ "-h ready -p 6380"
    assert args =~ "--tls ECHO"
    refute args =~ "secret"
    refute args =~ " -a "

    assert {:ok, auth} = Cli.run(cli, ["SHOW_AUTH"])
    assert auth =~ "auth=secret"
    assert auth =~ "args=-h ready -p 6380 --tls SHOW_AUTH"
    refute auth =~ " -a "

    assert Cli.run!(cli, ["ECHO"]) =~ "--tls ECHO"
    assert Cli.ping(cli)

    assert {:error, "failure"} = Cli.run(cli, ["FAIL"])
    assert_raise RuntimeError, ~r/redis-cli error: failure/, fn -> Cli.run!(cli, ["FAIL"]) end
    refute Cli.ping(%{cli | host: "error"})
  end

  test "CLI readiness distinguishes ready, transient, failed, and unexpected peers", %{
    cli_bin: bin
  } do
    assert :ok = Cli.wait_for_ready(Cli.new(bin: bin, host: "ready"), 20)
    assert :ok = Cli.wait_for_ready(Cli.new(bin: bin, host: "ready"))

    assert {:error, {:unexpected_reply, "NOPE"}} =
             Cli.wait_for_ready(Cli.new(bin: bin, host: "unexpected"), 20)

    assert {:error, :timeout} = Cli.wait_for_ready(Cli.new(bin: bin, host: "loading"), 20)
    assert {:error, :timeout} = Cli.wait_for_ready(Cli.new(bin: bin, host: "busy"), 20)
    assert {:error, :timeout} = Cli.wait_for_ready(Cli.new(bin: bin, host: "error"), 20)
  end

  test "CLI cluster and sentinel helpers parse success and preserve errors", %{cli_bin: bin} do
    authenticated = Cli.new(bin: bin, password: "secret")

    assert {:ok, "cluster-created"} =
             Cli.cluster_create(authenticated, ["127.0.0.1:7000"], 1)

    assert {:error, "cluster-create-error"} =
             Cli.cluster_create(Cli.new(bin: bin), ["failnode"])

    assert {:ok, %{"cluster_state" => "ok", "orphan" => ""}} =
             Cli.cluster_info(Cli.new(bin: bin, host: "ready"))

    assert {:error, "cluster-info-error"} =
             Cli.cluster_info(Cli.new(bin: bin, host: "error"))

    assert {:ok, %{"name" => "mymaster", "orphan" => ""}} =
             Cli.sentinel_master(Cli.new(bin: bin, host: "ready"), "mymaster")

    assert {:error, "sentinel-error"} =
             Cli.sentinel_master(Cli.new(bin: bin, host: "error"), "mymaster")
  end

  test "private file helpers create, replace, and harden filesystem state", %{
    fixture_dir: fixture_dir
  } do
    private_dir = Path.join(fixture_dir, "private")
    private_file = Path.join(private_dir, "secret")
    missing_file = Path.join(private_dir, "missing")

    SecureFile.make_private_directory!(private_dir)
    assert file_mode(private_dir) == 0o700

    SecureFile.write_private!(private_file, "first")
    assert File.read!(private_file) == "first"
    assert file_mode(private_file) == 0o600

    File.chmod!(private_file, 0o644)
    SecureFile.write_private!(private_file, "second")
    assert File.read!(private_file) == "second"
    assert file_mode(private_file) == 0o600

    SecureFile.atomic_write_private!(private_file, "atomic")
    assert File.read!(private_file) == "atomic"
    assert file_mode(private_file) == 0o600
    assert Path.wildcard(Path.join(private_dir, ".*.tmp-*")) == []

    File.chmod!(private_file, 0o644)
    assert :ok = SecureFile.harden_private_file(private_file)
    assert file_mode(private_file) == 0o600
    assert :missing = SecureFile.harden_private_file(missing_file)

    assert {:error, {:not_a_regular_file, :directory}} =
             SecureFile.harden_private_file(private_dir)

    assert_raise ArgumentError, fn -> SecureFile.write_private!(private_dir, "nope") end
  end

  test "top-level availability and version helpers cover every result shape", context do
    assert Server.default_server_bin() == "redis-server"
    assert Server.default_server_bin(:core) == "redis-server"
    assert Server.default_server_bin(:full) == "redis-server"

    original_legacy_bin = System.get_env("REDIS_LEGACY_STACK_SERVER_BIN")
    System.put_env("REDIS_LEGACY_STACK_SERVER_BIN", context.version_bin)

    on_exit(fn ->
      if original_legacy_bin do
        System.put_env("REDIS_LEGACY_STACK_SERVER_BIN", original_legacy_bin)
      else
        System.delete_env("REDIS_LEGACY_STACK_SERVER_BIN")
      end
    end)

    assert Server.default_server_bin(:legacy_stack) == context.version_bin

    assert RedisServerWrapper.available?(context.version_bin)
    refute RedisServerWrapper.available?("definitely-missing-redis-binary")

    assert {:ok, "9.8.7"} = RedisServerWrapper.version(context.version_bin)
    assert {:ok, "custom redis build"} = RedisServerWrapper.version(context.plain_version_bin)
    assert {:error, output} = RedisServerWrapper.version(context.failing_version_bin)
    assert output =~ "version failed"

    assert {:error, {:binary_not_found, "definitely-missing-redis-binary"}} =
             RedisServerWrapper.version("definitely-missing-redis-binary")
  end

  test "Server reports missing binaries and managed/unmanaged startup failures" do
    assert {:error, {:binary_not_found, "missing-redis-server"}} =
             Server.start(name: :missing_binary_server, redis_server_bin: "missing-redis-server")

    assert {:error, {:binary_not_found, "missing-redis-cli"}} =
             Server.start(redis_cli_bin: "missing-redis-cli")

    assert {:error, {:invalid_distribution, :unknown}} =
             Server.start(distribution: :unknown)

    assert {:error, {:server_start_failed, 6440, _status, _output}} =
             Server.start(port: 6440, managed: false, redis_server_bin: "false")

    assert {:error, {:server_start_timeout, 6441}} =
             Server.start(port: 6441, managed: true, redis_server_bin: "false", timeout: 20)
  end

  test "legacy Stack module discovery is explicit", %{fixture_dir: fixture_dir} do
    prefix = Path.join(fixture_dir, "legacy-stack")
    bin_dir = Path.join(prefix, "bin")
    lib_dir = Path.join(prefix, "lib")
    File.mkdir_p!(bin_dir)
    File.mkdir_p!(lib_dir)

    redis_server = Path.join(bin_dir, "redis-server")
    File.ln_s!(System.find_executable("redis-server"), redis_server)
    File.write!(Path.join(lib_dir, "rediscompat.so"), "not a module")

    {:ok, core_server} =
      Server.start_link(
        port: 6492,
        redis_server_bin: redis_server,
        distribution: :core
      )

    assert Server.ping(core_server)
    assert Server.stop(core_server) == :ok

    assert {:error, {:server_start_timeout, 6493}} =
             Server.start(
               port: 6493,
               redis_server_bin: redis_server,
               distribution: :legacy_stack,
               timeout: 100
             )
  end

  test "OS process helpers normalize executable results", %{fixture_dir: fixture_dir} do
    write_executable(
      fixture_dir,
      "kill",
      ~S"""
      #!/bin/sh
      if [ "$2" = "123" ]; then
        exit 0
      fi

      echo "signal failed"
      exit 2
      """
    )

    write_executable(
      fixture_dir,
      "lsof",
      "#!/bin/sh\nprintf '123\\ninvalid\\n456\\n'\n"
    )

    write_executable(
      fixture_dir,
      "ps",
      ~S"""
      #!/bin/sh
      if [ "$4" = "123" ]; then
        echo "1"
      else
        echo "42"
      fi
      """
    )

    original_path = System.get_env("PATH")
    on_exit(fn -> restore_path(original_path) end)
    System.put_env("PATH", fixture_dir)

    assert OSProcess.available?("kill")
    assert :ok = OSProcess.signal(123, :term)
    assert :ok = OSProcess.signal(123, :kill)
    assert :ok = OSProcess.signal(123, :stop)
    assert :ok = OSProcess.signal(123, :cont)

    assert {:error, {:signal_failed, :term, 999, 2, "signal failed"}} =
             OSProcess.signal(999, :term)

    assert OSProcess.alive?(123)
    refute OSProcess.alive?(999)
    refute OSProcess.alive?(nil)
    refute OSProcess.alive?(-1)
    assert OSProcess.orphaned?(123)
    refute OSProcess.orphaned?(999)
    assert {:ok, [123, 456]} = OSProcess.pids_on_port(6490)
  end

  test "missing kill and lsof do not crash process helpers or normal teardown", %{
    cli_bin: cli_bin
  } do
    redis_server_bin = System.find_executable("redis-server")
    redis_cli_bin = System.find_executable("redis-cli")
    false_bin = System.find_executable("false")
    original_path = System.get_env("PATH")
    empty_path = Path.join(System.tmp_dir!(), "redis-server-wrapper-no-process-tools")
    File.mkdir_p!(empty_path)

    on_exit(fn -> restore_path(original_path) end)

    System.put_env("PATH", empty_path)

    refute OSProcess.available?("kill")
    refute OSProcess.available?("lsof")
    assert {:error, {:executable_not_found, "kill"}} = OSProcess.signal(1, :term)
    assert {:error, {:executable_not_found, "lsof"}} = OSProcess.pids_on_port(6490)
    assert is_boolean(OSProcess.alive?(String.to_integer(System.pid())))
    assert is_boolean(OSProcess.orphaned?(String.to_integer(System.pid())))

    assert {:ok, server} =
             Server.start_link(
               port: 6490,
               redis_server_bin: redis_server_bin,
               redis_cli_bin: redis_cli_bin
             )

    assert :ok = Server.stop(server)

    assert {:error, {:node_start_failed, 7490, _reason}} =
             Cluster.start(
               masters: 1,
               base_port: 7490,
               timeout: 20,
               redis_server_bin: false_bin,
               redis_cli_bin: cli_bin
             )
  end

  test "cluster validates counts, ports, and timeouts before startup" do
    assert {:error, {:invalid_distribution, :unknown}} =
             Cluster.start(distribution: :unknown)

    assert {:error, {:invalid_option, :masters, 0}} = Cluster.start(masters: 0)

    assert {:error, {:invalid_option, :replicas_per_master, -1}} =
             Cluster.start(replicas_per_master: -1)

    assert {:error, {:invalid_option, :timeout, 0}} = Cluster.start(timeout: 0)
    assert {:error, {:invalid_option, :base_port, 0}} = Cluster.start(base_port: 0)

    assert {:error, {:invalid_port_range, :cluster_nodes, 65_535, 65_536}} =
             Cluster.start(masters: 2, base_port: 65_535)

    assert {:error, {:invalid_port_range, :cluster_bus, 65_536, 65_536}} =
             Cluster.start(masters: 1, base_port: 55_536)

    assert {:error,
            {:overlapping_port_ranges, :cluster_nodes, 1, 10_001, :cluster_bus, 10_001, 20_001}} =
             Cluster.start(masters: 10_001, base_port: 1)
  end

  test "sentinel validates counts, quorum, ports, and timeouts before startup" do
    assert {:error, {:invalid_distribution, :unknown}} =
             Sentinel.start(distribution: :unknown)

    assert {:error, {:invalid_option, :replicas, -1}} = Sentinel.start(replicas: -1)
    assert {:error, {:invalid_option, :sentinels, 0}} = Sentinel.start(sentinels: 0)
    assert {:error, {:invalid_quorum, 4, 3}} = Sentinel.start(quorum: 4)
    assert {:error, {:invalid_option, :timeout, 0}} = Sentinel.start(timeout: 0)
    assert {:error, {:invalid_option, :master_port, "bad"}} = Sentinel.start(master_port: "bad")

    assert {:error, {:invalid_option, :sentinel_base_port, 0}} =
             Sentinel.start(sentinel_base_port: 0)

    assert {:error, {:invalid_option, :replicas_base_port, "bad"}} =
             Sentinel.start(replicas: 1, replica_base_port: "bad")

    assert {:error, {:invalid_port_range, :replicas, 65_535, 65_536}} =
             Sentinel.start(replicas: 2, replica_base_port: 65_535)

    assert {:error, {:invalid_port_range, :sentinels, 65_535, 65_536}} =
             Sentinel.start(sentinels: 2, sentinel_base_port: 65_535, quorum: 1)

    assert {:error, {:overlapping_ports, [6390, 6391, 6391]}} =
             Sentinel.start(
               replicas: 1,
               sentinels: 1,
               replica_base_port: 6391,
               sentinel_base_port: 6391,
               quorum: 1
             )
  end

  defp write_executable(dir, name, contents) do
    path = Path.join(dir, name)
    File.write!(path, contents)
    File.chmod!(path, 0o755)
    path
  end

  defp file_mode(path), do: Bitwise.band(File.stat!(path).mode, 0o777)

  defp restore_path(nil), do: System.delete_env("PATH")
  defp restore_path(path), do: System.put_env("PATH", path)
end
