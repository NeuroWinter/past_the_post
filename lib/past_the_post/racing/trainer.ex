defmodule PastThePost.Racing.Trainer do
  use Ecto.Schema
  schema "trainers" do
    field :name, :string
    timestamps()
  end
end
