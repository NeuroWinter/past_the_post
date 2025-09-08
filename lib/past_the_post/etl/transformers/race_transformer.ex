defmodule PastThePost.ETL.RaceTransformer do
  @moduledoc """
  Transforms race data from TAB API format to internal format.
  
  Handles the complexities of various TAB API response formats and
  normalizes them into a consistent internal representation.
  """

  alias PastThePost.ETL.{DataParser, Error}
  
  @type race_data :: %{
    date: Date.t(),
    track: binary(),
    country: binary(),
    distance_m: integer(),
    going: nil | binary(),
    class: nil | binary(),
    race_number: integer(),
    runners: [map()]
  }

  @doc """
  Transforms a meeting and its races from TAB API format.
  
  ## Examples
  
      iex> transform_meeting(~D[2024-01-01], meeting_data, races_data)
      {:ok, [%{date: ~D[2024-01-01], track: "Ellerslie", ...}]}
      
      iex> transform_meeting(~D[2024-01-01], %{}, [])
      {:ok, []}
  """
  @spec transform_meeting(Date.t(), map(), [map()]) :: {:ok, [race_data()]} | {:error, Error.t()}
  def transform_meeting(date, meeting, races) when is_list(races) do
    with {:ok, track} <- extract_track_name(meeting),
         {:ok, country} <- extract_country(meeting) do
      
      transformed_races = 
        races
        |> Enum.map(&transform_race(&1, date, track, country))
        |> Enum.reject(&match?({:error, _}, &1))
        |> Enum.map(fn {:ok, race} -> race end)
      
      {:ok, transformed_races}
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    error ->
      {:error, Error.parse_error("Failed to transform meeting", %{
        error: inspect(error),
        meeting: meeting,
        date: date
      })}
  end

  # Transforms a single race from TAB API format.
  @spec transform_race(map(), Date.t(), binary(), binary()) :: {:ok, race_data()} | {:error, Error.t()}
  defp transform_race(race_map, date, track, country) do
    with {:ok, distance} <- extract_distance(race_map),
         {:ok, race_number} <- extract_race_number(race_map),
         {:ok, runners} <- extract_runners(race_map) do
      
      {:ok, %{
        date: date,
        track: track,
        country: country,
        distance_m: distance,
        going: extract_going(race_map),
        class: extract_class(race_map),
        race_number: race_number,
        runners: runners
      }}
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    error ->
      {:error, Error.parse_error("Failed to transform race", %{
        error: inspect(error),
        race: race_map,
        track: track,
        date: date
      })}
  end

  # Extract track name from meeting data
  defp extract_track_name(meeting) do
    case DataParser.extract_string(meeting, ["venue", "name", "track"]) do
      nil -> {:error, Error.validation_error("Missing track name", %{meeting: meeting})}
      track -> {:ok, track}
    end
  end

  # Extract country from meeting data
  defp extract_country(meeting) do
    country = DataParser.normalize_country(Map.get(meeting, "country"))
    {:ok, country}
  end

  # Extract distance with validation
  defp extract_distance(race_map) do
    case DataParser.extract_number(race_map, ["distance", "distanceMeters", "length"]) do
      nil -> {:error, Error.validation_error("Missing race distance", %{race: race_map})}
      distance when distance <= 0 -> 
        {:error, Error.validation_error("Invalid race distance", %{distance: distance, race: race_map})}
      distance -> {:ok, distance}
    end
  end

  # Extract race number with validation
  defp extract_race_number(race_map) do
    case DataParser.to_int(Map.get(race_map, "number")) do
      nil -> {:error, Error.validation_error("Missing race number", %{race: race_map})}
      race_number when race_number <= 0 ->
        {:error, Error.validation_error("Invalid race number", %{race_number: race_number, race: race_map})}
      race_number -> {:ok, race_number}
    end
  end

  # Extract going/track condition
  defp extract_going(race_map) do
    DataParser.extract_string(race_map, ["trackCondition", "going", "track"])
  end

  # Extract race class
  defp extract_class(race_map) do
    DataParser.extract_string(race_map, ["class", "raceClass"])
  end

  # Extract and normalize runners from various API formats
  defp extract_runners(race_map) do
    runners = 
      cond do
        has_runners?(race_map, "results") -> 
          Map.get(race_map, "results", [])
        
        has_runners?(race_map, "runners") -> 
          Map.get(race_map, "runners", [])
        
        has_runners?(race_map, "entries") -> 
          Map.get(race_map, "entries", [])
        
        has_placings_or_also_ran?(race_map) ->
          normalize_runners_from_results(race_map)
        
        true -> 
          []
      end

    {:ok, runners}
  end

  # Check if race has runners in a specific field
  defp has_runners?(race_map, field) do
    case Map.get(race_map, field) do
      runners when is_list(runners) and length(runners) > 0 -> true
      _ -> false
    end
  end

  # Check if race has results in placings/also_ran format
  defp has_placings_or_also_ran?(race_map) do
    has_runners?(race_map, "placings") or has_runners?(race_map, "also_ran")
  end

  # Convert placings/also_ran format to standard runner format
  defp normalize_runners_from_results(race_map) do
    placings = Map.get(race_map, "placings", [])
    also_ran = Map.get(race_map, "also_ran", [])

    placed_runners = Enum.map(placings, &normalize_placing_to_runner/1)
    other_runners = Enum.map(also_ran, &normalize_also_ran_to_runner/1)

    placed_runners ++ other_runners
  end

  # Convert a placing entry to standard runner format
  defp normalize_placing_to_runner(placing) do
    %{
      "horse" => %{"name" => Map.get(placing, "name")},
      "jockey" => Map.get(placing, "jockey"),
      "placing" => Map.get(placing, "rank"),
      "margin" => Map.get(placing, "distance"),
      "barrier" => nil,
      "weight" => nil,
      "trainer" => nil,
      "fixedOdds" => nil,
      "sp" => nil,
      "betfairSP" => nil
    }
  end

  # Convert an also_ran entry to standard runner format
  defp normalize_also_ran_to_runner(also_ran) do
    finish_pos = Map.get(also_ran, "finish_position", 0)
    
    %{
      "horse" => %{"name" => Map.get(also_ran, "name")},
      "jockey" => Map.get(also_ran, "jockey"),
      "placing" => (if finish_pos == 0, do: nil, else: finish_pos),
      "margin" => Map.get(also_ran, "distance"),
      "barrier" => Map.get(also_ran, "barrier"),
      "weight" => Map.get(also_ran, "weight"),
      "trainer" => nil,
      "fixedOdds" => nil,
      "sp" => nil,
      "betfairSP" => nil
    }
  end

  @doc """
  Validates transformed race data for completeness and consistency.
  """
  @spec validate_race(race_data()) :: :ok | {:error, Error.t()}
  def validate_race(race_data) do
    with :ok <- validate_required_fields(race_data),
         :ok <- validate_race_constraints(race_data),
         :ok <- validate_runners(race_data.runners) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Validate that all required fields are present
  defp validate_required_fields(race_data) do
    required_fields = [:date, :track, :country, :distance_m, :race_number]
    
    missing_fields = 
      required_fields
      |> Enum.reject(fn field -> Map.has_key?(race_data, field) and Map.get(race_data, field) != nil end)
    
    case missing_fields do
      [] -> :ok
      fields -> {:error, Error.validation_error("Missing required fields", %{missing: fields, race: race_data})}
    end
  end

  # Validate race business rules
  defp validate_race_constraints(race_data) do
    cond do
      race_data.distance_m <= 0 ->
        {:error, Error.validation_error("Invalid distance", %{distance: race_data.distance_m})}
      
      race_data.race_number <= 0 ->
        {:error, Error.validation_error("Invalid race number", %{race_number: race_data.race_number})}
      
      String.length(race_data.country) < 2 ->
        {:error, Error.validation_error("Invalid country code", %{country: race_data.country})}
      
      true -> :ok
    end
  end

  # Validate that we have valid runners
  defp validate_runners(runners) when is_list(runners) and length(runners) == 0 do
    {:error, Error.validation_error("Race has no runners", %{})}
  end
  defp validate_runners(runners) when is_list(runners) do
    invalid_runners = 
      runners
      |> Enum.with_index()
      |> Enum.reject(fn {runner, _index} -> valid_runner?(runner) end)
    
    case invalid_runners do
      [] -> :ok
      invalid -> {:error, Error.validation_error("Invalid runners found", %{invalid_runners: invalid})}
    end
  end
  defp validate_runners(_), do: {:error, Error.validation_error("Runners must be a list", %{})}

  # Check if a runner has minimum required data
  defp valid_runner?(runner) when is_map(runner) do
    horse_name = get_in(runner, ["horse", "name"]) || Map.get(runner, "name")
    is_binary(horse_name) and String.trim(horse_name) != ""
  end
  defp valid_runner?(_), do: false
end
