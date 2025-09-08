defmodule PastThePost.Repo.Migrations.AddUniqueIndexOnEntries do
  use Ecto.Migration
  def change do
    create_if_not_exists unique_index(:entries, [:race_id, :horse_id],
      name: :entries_race_id_horse_id_index
    )
  end
end
