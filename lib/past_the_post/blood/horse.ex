defmodule PastThePost.Blood.Horse do
  use Ecto.Schema
  import Ecto.Changeset

  schema "horses" do
    field :name, :string
    field :country, :string
    field :year_foaled, :integer
    field :sex, :string
    belongs_to :sire, __MODULE__
    belongs_to :dam, __MODULE__
    belongs_to :damsire, __MODULE__
    timestamps()
  end

  def changeset(horse, attrs) do
    horse
    |> cast(attrs, [:name, :country, :year_foaled, :sex, :sire_id, :dam_id, :damsire_id])
    |> validate_required([:name])
    |> unique_constraint([:name, :country, :year_foaled])
  end
end

