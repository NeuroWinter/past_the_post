defmodule PastThePost.Repo.Migrations.AddUniqueIndexOnRaces do
  use Ecto.Migration

  def change do
    # Old non-unique index is redundant; drop it if it exists
    drop_if_exists index(:races, [:date, :track], name: :races_date_track_index)

    # Required by your upsert: (:date, :track, :race_number)
    create unique_index(:races, [:date, :track, :race_number],
      name: :races_date_track_number_index
    )
  end
end
