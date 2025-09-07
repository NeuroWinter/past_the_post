defmodule PastThePost.Racing.Race do
  use Ecto.Schema
  import Ecto.Changeset
  schema "races" do
    field :date, :date
    field :track, :string
    field :country, :string
    field :distance_m, :integer
    field :surface, :string
    field :going, :string
    field :class, :string
    field :race_number, :integer
    timestamps()
  end
  def changeset(race, attrs), do:
    cast(race, attrs, [:date,:track,:country,:distance_m,:surface,:going,:class,:race_number])
end

