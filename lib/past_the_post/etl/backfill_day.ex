defmodule PastThePost.ETL.BackfillDay do
  @moduledoc """
  Oban worker for processing daily racing data.
  
  Orchestrates the ETL pipeline for a single day's racing data,
  handling API fetching, data transformation, and database persistence.
  """

  use Oban.Worker, queue: :etl, max_attempts: 5

  require Logger

  alias PastThePost.ETL.{
    TabClient,
    RaceTransformer,
    RunnerTransformer,
    Error
  }
  alias PastThePost.Data.{HorseUpsert, ParticipantUpsert}
  alias PastThePost.{Repo, Racing.Race, Racing.Entry}

  @doc """
  Processes racing data for a single date.
  
  ## Job Arguments
  
  - `date`: ISO8601 date string (required)
  
  ## Process Flow
  
  1. Fetch schedule from TAB API
  2. For each meeting:
     - Fetch results data
     - Transform race and runner data
     - Persist to database
  3. Log processing statistics
  """
  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"date" => iso_date}}) do
    with {:ok, date} <- parse_date(iso_date),
         {:ok, schedule} <- fetch_schedule(date),
         {:ok, stats} <- process_meetings(date, schedule) do
      
      log_processing_success(date, stats)
      :ok
    else
      {:error, %Error{type: :rate_limit_error} = error} ->
        log_rate_limit_error(error)
        {:snooze, error.retry_after}
      
      {:error, %Error{} = error} when error.type in [:api_error, :network_error] ->
        log_retriable_error(error)
        {:error, error.message}
      
      {:error, %Error{} = error} ->
        log_processing_error(error)
        {:discard, error.message}
      
      {:error, other_error} ->
        log_unexpected_error(other_error, iso_date)
        {:error, "Unexpected error occurred"}
    end
  end

  # Parse and validate the date parameter
  defp parse_date(iso_date) do
    case Date.from_iso8601(iso_date) do
      {:ok, date} -> {:ok, date}
      {:error, _} -> {:error, Error.validation_error("Invalid date format", %{date: iso_date})}
    end
  end

  # Fetch the racing schedule for a date
  defp fetch_schedule(date) do
    try do
      schedule = TabClient.schedule!(date)
      {:ok, schedule}
    rescue
      error ->
        {:error, Error.api_error("Failed to fetch schedule", %{
          date: date,
          error: inspect(error)
        })}
    end
  end

  # Process all meetings for a date
  defp process_meetings(date, schedule) do
    meetings = Map.get(schedule, "meetings", [])
    
    stats = %{
      meetings_processed: 0,
      races_processed: 0,
      entries_processed: 0,
      errors: []
    }

    final_stats = 
      Enum.reduce(meetings, stats, fn meeting, acc_stats ->
        case process_meeting(date, meeting) do
          {:ok, meeting_stats} ->
            %{
              meetings_processed: acc_stats.meetings_processed + 1,
              races_processed: acc_stats.races_processed + meeting_stats.races_processed,
              entries_processed: acc_stats.entries_processed + meeting_stats.entries_processed,
              errors: acc_stats.errors
            }
          
          {:error, error} ->
            Logger.warning("Failed to process meeting: #{Error.message(error)}", 
              error_context: Error.format_for_logging(error))
            
            %{acc_stats | errors: [error | acc_stats.errors]}
        end
      end)

    {:ok, final_stats}
  end

  # Process a single meeting
  defp process_meeting(date, meeting) do
    with {:ok, meetno} <- extract_meeting_number(meeting),
         {:ok, results} <- fetch_meeting_results(date, meetno),
         {:ok, races_data} <- extract_races_data(meeting, results),
         {:ok, transformed_races} <- RaceTransformer.transform_meeting(date, meeting, races_data),
         {:ok, meeting_stats} <- persist_meeting_data(transformed_races) do
      {:ok, meeting_stats}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Extract meeting number from meeting data
  defp extract_meeting_number(meeting) do
    meetno = meeting["number"] || meeting["meetNo"] || meeting["meetno"]
    
    case meetno do
      nil -> {:error, Error.validation_error("Missing meeting number", %{meeting: meeting})}
      number -> {:ok, number}
    end
  end

  # Fetch results for a specific meeting
  defp fetch_meeting_results(date, meetno) do
    try do
      results = TabClient.results_for_meeting!(date, meetno)
      {:ok, results}
    rescue
      error ->
        Logger.warning("Failed to fetch results for meeting #{meetno} on #{date}: #{inspect(error)}")
        # Return empty results rather than failing the whole day
        {:ok, %{"meetings" => []}}
    end
  end

  # Extract races data from meeting/results
  defp extract_races_data(meeting, results) do
    races = 
      cond do
        # Handle results API response (nested under meetings)
        is_map(results) and Map.has_key?(results, "meetings") ->
          results_meetings = results["meetings"] || []
          meetno = meeting["number"] || meeting["meetNo"] || meeting["meetno"]
          
          matching_meeting = Enum.find(results_meetings, fn m ->
            (m["number"] || m["meetNo"] || m["meetno"]) == meetno
          end)
          
          if matching_meeting, do: matching_meeting["races"] || [], else: []

        # Handle schedule API response (races directly under meeting)
        is_map(meeting) and Map.has_key?(meeting, "races") ->
          meeting["races"] || []

        true ->
          []
      end

    {:ok, races}
  end

  # Persist transformed race data to database
  defp persist_meeting_data(transformed_races) do
    Repo.transaction(fn ->
      stats = %{races_processed: 0, entries_processed: 0}
      
      Enum.reduce(transformed_races, stats, fn race_data, acc_stats ->
        case persist_race_data(race_data) do
          {:ok, race_stats} ->
            %{
              races_processed: acc_stats.races_processed + 1,
              entries_processed: acc_stats.entries_processed + race_stats.entries_processed
            }
          
          {:error, error} ->
            Logger.error("Failed to persist race data: #{Error.message(error)}")
            acc_stats
        end
      end)
    end)
  rescue
    error ->
      {:error, Error.database_error("Transaction failed during meeting persistence", %{
        error: inspect(error)
      })}
  end

  # Persist a single race and its entries
  defp persist_race_data(race_data) do
    with {:ok, race} <- upsert_race(race_data),
         {:ok, runners} <- transform_runners(race_data),
         {:ok, participants} <- ParticipantUpsert.batch_upsert_from_runners(runners),
         {:ok, entries_count} <- persist_race_entries(race, runners, participants) do
      {:ok, %{entries_processed: entries_count}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Upsert race record
  defp upsert_race(race_data) do
    try do
      race = Repo.insert!(%Race{
        date: race_data.date,
        track: race_data.track,
        country: race_data.country,
        distance_m: race_data.distance_m,
        going: race_data.going,
        class: race_data.class,
        race_number: race_data.race_number
      },
      on_conflict: {:replace, [:distance_m, :going, :class]},
      conflict_target: [:date, :track, :race_number])

      {:ok, race}
    rescue
      error ->
        {:error, Error.database_error("Failed to upsert race", %{
          error: inspect(error),
          race: race_data
        })}
    end
  end

  # Transform runners using the breeding information
  defp transform_runners(race_data) do
    # Extract breeding info from the first race data if available
    breeding_info = extract_breeding_info(race_data)
    
    case RunnerTransformer.transform_runners(race_data.runners, breeding_info) do
      {:ok, runners} -> {:ok, runners}
      {:error, reason} -> {:error, reason}
    end
  end

  # Extract breeding information from race data
  defp extract_breeding_info(_race_data) do
    # Look for breeding info in the original race data
    # This might be stored in a winnersbreeding field or similar
    nil # For now, return nil - can be enhanced later
  end

  # Persist race entries with horses and participants
  defp persist_race_entries(race, runners, participants) do
    entries_processed = 
      Enum.reduce(runners, 0, fn runner, count ->
        case persist_single_entry(race, runner, participants) do
          {:ok, _entry} -> count + 1
          {:error, error} ->
            Logger.warning("Failed to persist entry: #{Error.message(error)}")
            count
        end
      end)

    {:ok, entries_processed}
  end

  # Persist a single race entry
  defp persist_single_entry(race, runner, participants) do
    with {:ok, horse} <- upsert_runner_horse(runner),
         {:ok, entry} <- upsert_race_entry(race, horse, runner, participants) do
      {:ok, entry}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Upsert horse for a runner
  defp upsert_runner_horse(runner) do
    horse_attrs = %{
      name: runner.horse_name,
      country: runner.horse_country,
      year_foaled: runner.horse_year_foaled,
      sex: runner.horse_sex
    }

    if runner.sire_name && runner.dam_name do
      # Horse with breeding info
      bloodline_attrs = %{
        sire_name: runner.sire_name,
        dam_name: runner.dam_name,
        damsire_name: nil
      }
      HorseUpsert.upsert_with_bloodline(horse_attrs, bloodline_attrs)
    else
      # Simple horse upsert
      HorseUpsert.upsert_simple(horse_attrs)
    end
  end

  # Upsert race entry
  defp upsert_race_entry(race, horse, runner, participants) do
    trainer = Map.get(participants.trainers, runner.trainer_name)
    jockey = Map.get(participants.jockeys, runner.jockey_name)

    try do
      entry = Repo.insert!(%Entry{
        race_id: race.id,
        horse_id: horse.id,
        trainer_id: trainer && trainer.id,
        jockey_id: jockey && jockey.id,
        barrier: runner.barrier,
        weight_kg: runner.weight_kg,
        finishing_pos: runner.finishing_pos,
        margin_l: runner.margin_l,
        sp_odds: runner.sp_odds,
        bf_sp: runner.bf_sp
      },
      on_conflict: {:replace_all_except, [:id, :race_id, :horse_id, :inserted_at]},
      conflict_target: [:race_id, :horse_id])

      {:ok, entry}
    rescue
      error ->
        {:error, Error.database_error("Failed to upsert entry", %{
          error: inspect(error),
          race_id: race.id,
          horse_id: horse.id
        })}
    end
  end

  # Logging functions

  defp log_processing_success(date, stats) do
    Logger.info("Successfully processed racing data for #{date}", %{
      date: date,
      meetings_processed: stats.meetings_processed,
      races_processed: stats.races_processed,
      entries_processed: stats.entries_processed,
      errors_count: length(stats.errors)
    })
  end

  defp log_rate_limit_error(error) do
    Logger.warning("Rate limit hit, will retry", 
      error_context: Error.format_for_logging(error))
  end

  defp log_retriable_error(error) do
    Logger.warning("Retriable error occurred", 
      error_context: Error.format_for_logging(error))
  end

  defp log_processing_error(error) do
    Logger.error("Non-retriable error occurred", 
      error_context: Error.format_for_logging(error))
  end

  defp log_unexpected_error(error, iso_date) do
    Logger.error("Unexpected error during ETL processing", %{
      error: inspect(error),
      date: iso_date
    })
  end
end
