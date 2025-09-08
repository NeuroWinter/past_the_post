defmodule PastThePost.Racing.Entry do
  use Ecto.Schema
  import Ecto.Changeset
  schema "entries" do
    belongs_to :race, PastThePost.Racing.Race
    belongs_to :horse, PastThePost.Blood.Horse
    belongs_to :trainer, PastThePost.Racing.Trainer
    belongs_to :jockey, PastThePost.Racing.Jockey
    field :barrier, :integer
    field :weight_kg, :float
    field :finishing_pos, :integer
    field :margin_l, :float
    field :sp_odds, :float
    field :bf_sp, :float
    timestamps()
  end
  def changeset(e, attrs), do:
    cast(e, attrs, [:race_id,:horse_id,:trainer_id,:jockey_id,:barrier,:weight_kg,:finishing_pos,:margin_l,:sp_odds,:bf_sp])
end

