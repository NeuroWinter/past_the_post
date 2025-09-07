defmodule PastThePost.MixProject do
  use Mix.Project

  def project do
    [
      app: :past_the_post,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {PastThePost.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, ">= 0.0.0"},
      {:req, "~> 0.5"},
      {:oban, "~> 2.17"},
      {:jason, "~> 1.4"}
    ]
  end
end
