defmodule PastThePostTest do
  use ExUnit.Case
  doctest PastThePost

  test "greets the world" do
    assert PastThePost.hello() == :world
  end
end
