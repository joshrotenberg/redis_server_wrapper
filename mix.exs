defmodule RedisServerWrapper.MixProject do
  use Mix.Project

  def project do
    [
      app: :redis_server_wrapper,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Elixir wrapper for redis-server and redis-cli with GenServer process management",
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {RedisServerWrapper.Application, []}
    ]
  end

  defp deps do
    []
  end

  defp package do
    [
      licenses: ["MIT", "Apache-2.0"],
      links: %{}
    ]
  end
end
