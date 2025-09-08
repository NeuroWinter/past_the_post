defmodule PastThePost.Repo.Migrations.CreateCoreTables do
  use Ecto.Migration

  def change do
    execute("CREATE EXTENSION IF NOT EXISTS citext", "")
    create table(:horses) do
      add :name, :citext, null: false
      add :country, :string, size: 3
      add :year_foaled, :integer
      add :sex, :string, size: 1
      add :sire_id, references(:horses, on_delete: :nilify_all)
      add :dam_id,  references(:horses, on_delete: :nilify_all)
      add :damsire_id, references(:horses, on_delete: :nilify_all)
      timestamps()
    end

    create unique_index(:horses, [:name, :country, :year_foaled])

    create table(:trainers) do
      add :name, :citext, null: false
      timestamps()
    end

    create table(:jockeys) do
      add :name, :citext, null: false
      timestamps()
    end

    create table(:races) do
      add :date, :date, null: false
      add :track, :citext, null: false
      add :country, :string, size: 2, null: false
      add :distance_m, :integer, null: false
      add :surface, :string
      add :going, :string
      add :class, :string
      add :race_number, :integer
      timestamps()
    end

    create index(:races, [:date, :track])

    create table(:entries) do
      add :race_id, references(:races, on_delete: :delete_all), null: false
      add :horse_id, references(:horses, on_delete: :delete_all), null: false
      add :trainer_id, references(:trainers, on_delete: :nilify_all)
      add :jockey_id, references(:jockeys, on_delete: :nilify_all)
      add :barrier, :integer
      add :weight_kg, :float
      add :finishing_pos, :integer
      add :margin_l, :float
      add :sp_odds, :float
      add :bf_sp, :float
      timestamps()
    end

    create unique_index(:entries, [:race_id, :horse_id])
    create index(:entries, [:horse_id])
    create index(:entries, [:trainer_id])
    create index(:entries, [:jockey_id])
  end
end
