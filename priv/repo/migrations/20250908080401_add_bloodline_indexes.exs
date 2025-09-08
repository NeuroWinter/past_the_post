defmodule PastThePost.Repo.Migrations.AddBloodlineIndexes do
  use Ecto.Migration

  def change do
    create index(:horses, [:sire_id])
    create index(:horses, [:dam_id])
    create index(:horses, [:damsire_id])
    # Compound indexes for common bloodline queries
    create index(:horses, [:sire_id, :dam_id])
    create index(:horses, [:sex, :sire_id])
  end
end
