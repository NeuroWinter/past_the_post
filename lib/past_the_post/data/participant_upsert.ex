defmodule PastThePost.Data.ParticipantUpsert do
  @moduledoc """
  Handles upsert operations for racing participants (trainers and jockeys).
  
  Provides efficient bulk operations for creating and updating trainer
  and jockey records.
  """

  import Ecto.Query
  alias PastThePost.Repo
  alias PastThePost.Racing.{Trainer, Jockey}
  alias PastThePost.ETL.Error

  @doc """
  Upserts a trainer by name.
  
  ## Examples
  
      iex> upsert_trainer("J TRAINER")
      {:ok, %Trainer{}}
      
      iex> upsert_trainer(nil)
      {:ok, nil}
  """
  @spec upsert_trainer(nil | binary()) :: {:ok, nil | Trainer.t()} | {:error, Error.t()}
  def upsert_trainer(nil), do: {:ok, nil}
  def upsert_trainer(name) when is_binary(name) do
    upsert_participant(Trainer, name, "trainer")
  end

  @doc """
  Upserts a jockey by name.
  
  ## Examples
  
      iex> upsert_jockey("S JOCKEY")
      {:ok, %Jockey{}}
      
      iex> upsert_jockey("")
      {:ok, nil}
  """
  @spec upsert_jockey(nil | binary()) :: {:ok, nil | Jockey.t()} | {:error, Error.t()}
  def upsert_jockey(nil), do: {:ok, nil}
  def upsert_jockey(name) when is_binary(name) do
    upsert_participant(Jockey, name, "jockey")
  end

  @doc """
  Bulk upserts multiple trainers efficiently.
  """
  @spec bulk_upsert_trainers([binary()]) :: {:ok, [Trainer.t()]} | {:error, Error.t()}
  def bulk_upsert_trainers(names) when is_list(names) do
    bulk_upsert_participants(Trainer, names, "trainers")
  end

  @doc """
  Bulk upserts multiple jockeys efficiently.
  """
  @spec bulk_upsert_jockeys([binary()]) :: {:ok, [Jockey.t()]} | {:error, Error.t()}
  def bulk_upsert_jockeys(names) when is_list(names) do
    bulk_upsert_participants(Jockey, names, "jockeys")
  end

  @doc """
  Finds trainers by names and returns a name-to-trainer map.
  """
  @spec find_trainers_by_names([binary()]) :: {:ok, %{binary() => Trainer.t()}} | {:error, Error.t()}
  def find_trainers_by_names(names) when is_list(names) do
    find_participants_by_names(Trainer, names, "trainers")
  end

  @doc """
  Finds jockeys by names and returns a name-to-jockey map.
  """
  @spec find_jockeys_by_names([binary()]) :: {:ok, %{binary() => Jockey.t()}} | {:error, Error.t()}
  def find_jockeys_by_names(names) when is_list(names) do
    find_participants_by_names(Jockey, names, "jockeys")
  end

  @doc """
  Batch upserts participants (trainers and jockeys) from runner data.
  
  Extracts all unique trainer and jockey names, then performs bulk upserts.
  Returns maps for efficient lookup during entry creation.
  """
  @spec batch_upsert_from_runners([map()]) :: {:ok, %{trainers: map(), jockeys: map()}} | {:error, Error.t()}
  def batch_upsert_from_runners(runners) when is_list(runners) do
    try do
      # Extract unique names
      trainer_names = 
        runners
        |> Enum.map(& &1.trainer_name)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      jockey_names = 
        runners
        |> Enum.map(& &1.jockey_name)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      # Perform bulk upserts
      with {:ok, trainers} <- bulk_upsert_trainers(trainer_names),
           {:ok, jockeys} <- bulk_upsert_jockeys(jockey_names) do
        
        # Create lookup maps
        trainer_map = Map.new(trainers, fn t -> {t.name, t} end)
        jockey_map = Map.new(jockeys, fn j -> {j.name, j} end)

        {:ok, %{trainers: trainer_map, jockeys: jockey_map}}
      else
        {:error, reason} -> {:error, reason}
      end
    rescue
      error ->
        {:error, Error.database_error("Failed to batch upsert participants", %{
          error: inspect(error),
          runners_count: length(runners)
        })}
    end
  end

  # Private helper functions

  # Generic participant upsert function
  defp upsert_participant(schema, name, type) do
    normalized_name = String.trim(name)
    
    case normalized_name do
      "" -> {:ok, nil}
      _ ->
        try do
          _participant = Repo.insert!(
            struct(schema, %{name: normalized_name}),
            on_conflict: :nothing,
            conflict_target: [:name]
          )

          # Fetch the actual record to ensure we have the ID
          result = Repo.get_by!(schema, name: normalized_name)
          {:ok, result}
        rescue
          error ->
            {:error, Error.database_error("Failed to upsert #{type}", %{
              error: inspect(error),
              name: normalized_name,
              type: type
            })}
        end
    end
  end

  # Generic bulk upsert for participants
  defp bulk_upsert_participants(schema, names, type) do
    normalized_names = 
      names
      |> Enum.map(&String.trim/1)
      |> Enum.reject(fn name -> name == "" end)
      |> Enum.uniq()

    case normalized_names do
      [] -> {:ok, []}
      _ ->
        Repo.transaction(fn ->
          try do
            # Insert all participants with conflict resolution
            Enum.each(normalized_names, fn name ->
              Repo.insert!(
                struct(schema, %{name: name}),
                on_conflict: :nothing,
                conflict_target: [:name]
              )
            end)

            # Fetch all inserted/updated participants
            participants = 
              from(p in schema, where: p.name in ^normalized_names)
              |> Repo.all()

            participants
          rescue
            error ->
              Repo.rollback(Error.database_error("Failed to bulk upsert #{type}", %{
                error: inspect(error),
                names_count: length(normalized_names),
                type: type
              }))
          end
        end)
    end
  end

  # Generic find by names function
  defp find_participants_by_names(schema, names, type) do
    try do
      participants = 
        from(p in schema, where: p.name in ^names)
        |> Repo.all()
        |> Map.new(fn participant -> {participant.name, participant} end)

      {:ok, participants}
    rescue
      error ->
        {:error, Error.database_error("Failed to find #{type} by names", %{
          error: inspect(error),
          names: names,
          type: type
        })}
    end
  end

  @doc """
  Gets statistics about participant data quality.
  
  Returns counts of participants with various data completeness levels.
  """
  @spec get_participant_stats() :: {:ok, map()} | {:error, Error.t()}
  def get_participant_stats do
    try do
      trainer_count = Repo.aggregate(Trainer, :count, :id)
      jockey_count = Repo.aggregate(Jockey, :count, :id)

      # Count participants with meaningful names (not just initials or single words)
      meaningful_trainers = 
        from(t in Trainer, where: fragment("length(?) > 3 AND ? ~ ' '", t.name, t.name))
        |> Repo.aggregate(:count, :id)

      meaningful_jockeys = 
        from(j in Jockey, where: fragment("length(?) > 3 AND ? ~ ' '", j.name, j.name))
        |> Repo.aggregate(:count, :id)

      {:ok, %{
        total_trainers: trainer_count,
        total_jockeys: jockey_count,
        meaningful_trainers: meaningful_trainers,
        meaningful_jockeys: meaningful_jockeys,
        trainer_completeness: if(trainer_count > 0, do: meaningful_trainers / trainer_count, else: 0),
        jockey_completeness: if(jockey_count > 0, do: meaningful_jockeys / jockey_count, else: 0)
      }}
    rescue
      error ->
        {:error, Error.database_error("Failed to get participant stats", %{
          error: inspect(error)
        })}
    end
  end

  @doc """
  Cleans up participant names by removing common prefixes and normalizing format.
  
  ## Examples
  
      iex> clean_participant_name("MR J SMITH")
      "J SMITH"
      
      iex> clean_participant_name("APPRENTICE S JONES")
      "S JONES"
  """
  @spec clean_participant_name(nil | binary()) :: nil | binary()
  def clean_participant_name(nil), do: nil
  def clean_participant_name(name) when is_binary(name) do
    name
    |> String.trim()
    |> String.upcase()
    |> remove_common_prefixes()
    |> normalize_whitespace()
    |> case do
      "" -> nil
      cleaned -> cleaned
    end
  end

  # Remove common prefixes from participant names
  defp remove_common_prefixes(name) do
    prefixes_to_remove = [
      "MR ", "MRS ", "MS ", "MISS ",
      "APPRENTICE ", "APP ", "A ",
      "CLAIMING ", "CLAIM "
    ]

    Enum.reduce(prefixes_to_remove, name, fn prefix, acc ->
      String.replace_prefix(acc, prefix, "")
    end)
  end

  # Normalize whitespace in names
  defp normalize_whitespace(name) do
    name
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
  end
end
