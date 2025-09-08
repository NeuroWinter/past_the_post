defmodule PastThePost.Repo.Migrations.AddUniqueIndexOnNames do
  use Ecto.Migration

  def change do
    create unique_index(:horses,  [:name], name: :horses_name_uniq)
    create unique_index(:jockeys, [:name], name: :jockeys_name_uniq)
    create unique_index(:trainers,[:name], name: :trainers_name_uniq)
  end
end
