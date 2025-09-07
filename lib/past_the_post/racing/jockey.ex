defmodule PastThePost.Racing.Jockey do
  use Ecto.Schema
  schema "jockeys" do
    field :name, :string
    timestamps()
  end
end
