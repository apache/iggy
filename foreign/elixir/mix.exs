defmodule IggyEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :iggy_ex,
      version: "0.1.0",
      elixir: "~> 1.14",
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "~> 0.36"}
    ]
  end

  defp package() do
    [
      files:
        ~w(lib native/iggy_nif/src native/iggy_nif/Cargo* native/iggy_nif/.cargo .formatter.exs mix.exs README* LICENSE*
                ),
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/apache/iggy",
        "Docs" => "https://hexdocs.pm/iggy_ex"
      }
    ]
  end
end
