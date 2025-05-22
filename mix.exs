defmodule Stompex.Mixfile do
  use Mix.Project

  def project do
    [app: :stompex,
     version: "0.2.3",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    []
  end
end
