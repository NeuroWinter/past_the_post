defmodule PastThePost.Data.HorseUpsert do
  @moduledoc """
  Handles upsert operations for horses and their bloodline relationships.
  
  Provides efficient bulk operations and proper handling of parent-child
  relationships in the horse pedigree data.
  """

  import Ecto.Query
  alias PastThePost.Repo
  alias PastThePost.Blood.Horse
  alias PastThePost.ETL.Error

  @type horse_attrs :: %{
    name: binary(),
    country: nil | binary(),
    year_foaled: nil | integer(),
    sex: nil | binary()
  }

  @type bloodline_attrs :: %{
    sire_name: nil | binary(),
    dam_name: nil | binary(),
    damsire_name: nil | binary()
  }

  @doc """
  Upserts a horse with its bloodline information.
  
  Creates parent horses if they don't exist, then creates/updates the main horse
  with proper parent references.
  
  ## Examples
  
      iex> upsert_with_bloodline(horse_attrs, %{sire_name: "SIRE", dam_name: "DAM"})
      {:ok, %Horse{}}
  """
  @spec upsert_with_bloodline(horse_attrs(), bloodline_attrs()) :: {:ok, Horse.t()} | {:error, Error.t()}
  def upsert_with_bloodline(horse_attrs, bloodline_attrs) do
    Repo.transaction(fn ->
      with {:ok, sire} <- maybe_upsert_parent(bloodline_attrs.sire_name),
           {:ok, dam} <- maybe_upsert_parent(bloodline_attrs.dam_name),
           {:ok, damsire} <- maybe_upsert_parent(bloodline_attrs.damsire_name),
           {:ok, horse} <- upsert_horse_with_parents(horse_attrs, sire, dam, damsire) do
        horse
      else
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  rescue
    error ->
      {:error, Error.database_error("Failed to upsert horse with bloodline", %{
        error: inspect(error),
        horse: horse_attrs,
        bloodline: bloodline_attrs
      })}
  end

  @doc """
  Upserts a simple horse without bloodline information.
  
  ## Examples
  
      iex> upsert_simple(%{name: "HORSE NAME", country: "NZ"})
      {:ok, %Horse{}}
  """
  @spec upsert_simple(horse_attrs()) :: {:ok, Horse.t()} | {:error, Error.t()}
  def upsert_simple(horse_attrs) do
    try do
      _horse = Repo.insert!(
        struct(Horse, horse_attrs),
        on_conflict: {:replace, [:country, :year_foaled, :sex]},
        conflict_target: [:name]
      )
      
      # Fetch the actual record to ensure we have the ID
      result = Repo.get_by!(Horse, name: horse_attrs.name)
      {:ok, result}
    rescue
      error ->
        {:error, Error.database_error("Failed to upsert horse", %{
          error: inspect(error),
          horse: horse_attrs
        })}
    end
  end

  @doc """
  Bulk upserts multiple horses efficiently.
  
  Uses a single database transaction and optimized conflict resolution.
  """
  @spec bulk_upsert([horse_attrs()]) :: {:ok, [Horse.t()]} | {:error, Error.t()}
  def bulk_upsert(horses_attrs) when is_list(horses_attrs) do
    Repo.transaction(fn ->
      try do
        # Insert all horses with conflict resolution
        Enum.each(horses_attrs, fn attrs ->
          Repo.insert!(
            struct(Horse, attrs),
            on_conflict: {:replace, [:country, :year_foaled, :sex]},
            conflict_target: [:name]
          )
        end)

        # Fetch all inserted/updated horses
        horse_names = Enum.map(horses_attrs, & &1.name)
        horses = 
          from(h in Horse, where: h.name in ^horse_names)
          |> Repo.all()

        horses
      rescue
        error ->
          Repo.rollback(Error.database_error("Failed to bulk upsert horses", %{
            error: inspect(error),
            horses_count: length(horses_attrs)
          }))
      end
    end)
  end

  @doc """
  Finds horses by names efficiently.
  
  Returns a map with name as key and Horse struct as value.
  """
  @spec find_by_names([binary()]) :: {:ok, %{binary() => Horse.t()}} | {:error, Error.t()}
  def find_by_names(names) when is_list(names) do
    try do
      horses = 
        from(h in Horse, where: h.name in ^names)
        |> Repo.all()
        |> Map.new(fn horse -> {horse.name, horse} end)

      {:ok, horses}
    rescue
      error ->
        {:error, Error.database_error("Failed to find horses by names", %{
          error: inspect(error),
          names: names
        })}
    end
  end

  # Private helper functions

  # Upserts a parent horse if the name is provided
  defp maybe_upsert_parent(nil), do: {:ok, nil}
  defp maybe_upsert_parent(name) when is_binary(name) and name != "" do
    case upsert_simple(%{name: String.trim(name)}) do
      {:ok, horse} -> {:ok, horse}
      {:error, reason} -> {:error, reason}
    end
  end
  defp maybe_upsert_parent(_), do: {:ok, nil}

  # Upserts horse with parent references
  defp upsert_horse_with_parents(horse_attrs, sire, dam, damsire) do
    enhanced_attrs = 
      horse_attrs
      |> Map.put(:sire_id, sire && sire.id)
      |> Map.put(:dam_id, dam && dam.id)
      |> Map.put(:damsire_id, damsire && damsire.id)

    try do
      _horse = Repo.insert!(
        struct(Horse, enhanced_attrs),
        on_conflict: {:replace, [:country, :year_foaled, :sex, :sire_id, :dam_id, :damsire_id]},
        conflict_target: [:name]
      )

      # Fetch the complete record
      result = Repo.get_by!(Horse, name: horse_attrs.name)
      {:ok, result}
    rescue
      error ->
        {:error, Error.database_error("Failed to upsert horse with parents", %{
          error: inspect(error),
          horse: enhanced_attrs
        })}
    end
  end

  @doc """
  Gets or creates a horse by name only.
  
  Useful for quick lookups where we don't have detailed horse information.
  """
  @spec get_or_create_by_name(binary()) :: {:ok, Horse.t()} | {:error, Error.t()}
  def get_or_create_by_name(name) when is_binary(name) do
    normalized_name = String.trim(name)
    
    case normalized_name do
      "" -> {:error, Error.validation_error("Horse name cannot be empty", %{name: name})}
      _ -> upsert_simple(%{name: normalized_name})
    end
  end

  @doc """
  Updates bloodline information for an existing horse.
  
  Useful when we get additional pedigree information after initial creation.
  """
  @spec update_bloodline(Horse.t(), bloodline_attrs()) :: {:ok, Horse.t()} | {:error, Error.t()}
  def update_bloodline(%Horse{} = horse, bloodline_attrs) do
    Repo.transaction(fn ->
      with {:ok, sire} <- maybe_upsert_parent(bloodline_attrs.sire_name),
           {:ok, dam} <- maybe_upsert_parent(bloodline_attrs.dam_name),
           {:ok, damsire} <- maybe_upsert_parent(bloodline_attrs.damsire_name) do
        
        changeset = Horse.changeset(horse, %{
          sire_id: sire && sire.id,
          dam_id: dam && dam.id,
          damsire_id: damsire && damsire.id
        })

        case Repo.update(changeset) do
          {:ok, updated_horse} -> updated_horse
          {:error, changeset} -> 
            Repo.rollback(Error.database_error("Failed to update horse bloodline", %{
              errors: changeset.errors,
              horse_id: horse.id
            }))
        end
      else
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end
end
