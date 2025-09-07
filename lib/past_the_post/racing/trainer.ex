defmodule PastThePost.Racing.Trainer do
  use Ecto.Schema
  schema "trainers" do
    field :name, :string
    timestamps()
  end
end

defmodule PastThePost.Racing.Jockey do
  use Ecto.Schema
  schema "jockeys" do
    field :name, :string
    timestamps()
  end
end

