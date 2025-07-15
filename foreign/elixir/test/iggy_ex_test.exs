defmodule IggyExTest do
  use ExUnit.Case
  doctest IggyEx

  test "greets the world" do
    assert IggyEx.hello() == :world
  end
end
