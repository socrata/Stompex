defmodule Stompex.Mixfile do
  use Mix.Project

  def project do
    [app: :stompex,
     version: "0.3.0",
     elixir: "~> 1.17",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end


  defp deps do
    [{:connection, "~> 1.0"}]
  end

end
