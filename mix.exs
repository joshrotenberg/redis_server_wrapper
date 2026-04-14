defmodule RedisServerWrapper.MixProject do
  use Mix.Project

  @version "0.6.0"
  @source_url "https://github.com/joshrotenberg/redis_server_wrapper"

  def project do
    [
      app: :redis_server_wrapper,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Manage redis-server processes from Elixir -- single instances, clusters, and sentinel topologies with GenServer lifecycle management",
      package: package(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url,
      dialyzer: [plt_file: {:no_warn, "_build/dev/dialyxir_#{System.otp_release()}.plt"}]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {RedisServerWrapper.Application, []}
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.35", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT", "Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end
