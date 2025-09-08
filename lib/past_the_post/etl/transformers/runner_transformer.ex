defmodule PastThePost.ETL.RunnerTransformer do
  @moduledoc """
  Transforms runner/entry data from TAB API format to internal format.
  
  Handles the transformation of individual horse entries within races,
  including breeding information and performance data.
  """

  alias PastThePost.ETL.{DataParser, BreedingParser, Error}

  @type runner_data :: %{
    horse_name: binary(),
    horse_country: nil | binary(),
    horse_year_foaled: nil | integer(),
    horse_sex: nil | binary(),
    sire_name: nil | binary(),
    dam_name: nil | binary(),
    trainer_name: nil | binary(),
    jockey_name: nil | binary(),
    barrier: nil | integer(),
    weight_kg: nil | float(),
    finishing_pos: nil | integer(),
    margin_l: nil | float(),
    sp_odds: nil | float(),
    bf_sp: nil | float()
  }

  @doc """
  Transforms a runner from TAB API format to internal format.
  
  ## Examples
  
      iex> transform_runner(runner_data, breeding_info)
      {:ok, %{horse_name: "HORSE NAME", trainer_name: "J TRAINER", ...}}
  """
  @spec transform_runner(map(), nil | binary()) :: {:ok, runner_data()} | {:error, Error.t()}
  def transform_runner(runner, breeding_info \\ nil) do
    with {:ok, horse_name} <- extract_horse_name(runner),
         {:ok, breeding} <- parse_breeding_information(breeding_info, runner) do
      
      {:ok, %{
        horse_name: horse_name,
        horse_country: extract_horse_country(runner),
        horse_year_foaled: extract_horse_year_foaled(runner),
        horse_sex: breeding.sex,
        sire_name: breeding.sire_name,
        dam_name: breeding.dam_name,
        trainer_name: extract_trainer_name(runner),
        jockey_name: extract_jockey_name(runner),
        barrier: extract_barrier(runner),
        weight_kg: extract_weight(runner),
        finishing_pos: extract_finishing_position(runner),
        margin_l: extract_margin(runner),
        sp_odds: extract_starting_price(runner),
        bf_sp: extract_betfair_sp(runner)
      }}
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    error ->
      {:error, Error.parse_error("Failed to transform runner", %{
        error: inspect(error),
        runner: runner,
        breeding_info: breeding_info
      })}
  end

  @doc """
  Transforms multiple runners, handling breeding info for winners.
  
  The breeding_info parameter typically contains information about the race winner.
  """
  @spec transform_runners([map()], nil | binary()) :: {:ok, [runner_data()]} | {:error, Error.t()}
  def transform_runners(runners, breeding_info \\ nil) when is_list(runners) do
    transformed_runners = 
      runners
      |> Enum.map(fn runner ->
        # Apply breeding info only to the winner (placing = 1)
        winner_breeding = if is_winner?(runner), do: breeding_info, else: nil
        transform_runner(runner, winner_breeding)
      end)
      |> Enum.reject(&match?({:error, _}, &1))
      |> Enum.map(fn {:ok, runner} -> runner end)

    {:ok, transformed_runners}
  rescue
    error ->
      {:error, Error.parse_error("Failed to transform runners", %{
        error: inspect(error),
        runners_count: length(runners)
      })}
  end

  # Extract horse name from runner data
  defp extract_horse_name(runner) do
    horse_name = 
      case runner do
        %{"horse" => %{"name" => name}} -> name
        %{"name" => name} -> name
        _ -> nil
      end

    case DataParser.normalize_string(horse_name) do
      nil -> {:error, Error.validation_error("Missing horse name", %{runner: runner})}
      name -> {:ok, name}
    end
  end

  # Extract horse country
  defp extract_horse_country(runner) do
    country = 
      case runner do
        %{"horse" => horse_map} -> Map.get(horse_map, "country")
        _ -> Map.get(runner, "country")
      end
    
    DataParser.normalize_country(country)
  end

  # Extract horse year of birth
  defp extract_horse_year_foaled(runner) do
    yob = 
      case runner do
        %{"horse" => horse_map} -> Map.get(horse_map, "yob")
        _ -> Map.get(runner, "yob")
      end
    
    DataParser.to_int(yob)
  end

  # Parse breeding information if available
  defp parse_breeding_information(nil, _runner), do: {:ok, BreedingParser.parse(nil)}
  defp parse_breeding_information(breeding_str, _runner) when is_binary(breeding_str) do
    {:ok, BreedingParser.parse(breeding_str)}
  end

  # Extract trainer name
  defp extract_trainer_name(runner) do
    DataParser.normalize_string(Map.get(runner, "trainer"))
  end

  # Extract jockey name
  defp extract_jockey_name(runner) do
    DataParser.normalize_string(Map.get(runner, "jockey"))
  end

  # Extract barrier number
  defp extract_barrier(runner) do
    DataParser.to_int(Map.get(runner, "barrier"))
  end

  # Extract weight in kg
  defp extract_weight(runner) do
    DataParser.to_float(Map.get(runner, "weight"))
  end

  # Extract finishing position
  defp extract_finishing_position(runner) do
    # Handle various field names for finishing position
    position = 
      Map.get(runner, "placing") ||
      Map.get(runner, "finishPosition") ||
      Map.get(runner, "rank")
    
    DataParser.to_int(position)
  end

  # Extract margin (beaten distance)
  defp extract_margin(runner) do
    DataParser.to_float(Map.get(runner, "margin"))
  end

  # Extract starting price odds
  defp extract_starting_price(runner) do
    odds = 
      Map.get(runner, "fixedOdds") ||
      Map.get(runner, "sp") ||
      Map.get(runner, "startingPrice")
    
    DataParser.to_float(odds)
  end

  # Extract Betfair starting price
  defp extract_betfair_sp(runner) do
    DataParser.to_float(Map.get(runner, "betfairSP"))
  end

  # Check if runner is the winner
  defp is_winner?(runner) do
    case extract_finishing_position(runner) do
      1 -> true
      _ -> false
    end
  end

  @doc """
  Validates a transformed runner for data quality.
  """
  @spec validate_runner(runner_data()) :: :ok | {:error, Error.t()}
  def validate_runner(runner_data) do
    with :ok <- validate_required_runner_fields(runner_data),
         :ok <- validate_runner_constraints(runner_data) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Validate required fields
  defp validate_required_runner_fields(runner_data) do
    case runner_data.horse_name do
      name when is_binary(name) and name != "" -> :ok
      _ -> {:error, Error.validation_error("Invalid horse name", %{runner: runner_data})}
    end
  end

  # Validate business constraints
  defp validate_runner_constraints(runner_data) do
    cond do
      not is_nil(runner_data.barrier) and runner_data.barrier <= 0 ->
        {:error, Error.validation_error("Invalid barrier number", %{barrier: runner_data.barrier})}
      
      not is_nil(runner_data.weight_kg) and runner_data.weight_kg <= 0 ->
        {:error, Error.validation_error("Invalid weight", %{weight: runner_data.weight_kg})}
      
      not is_nil(runner_data.finishing_pos) and runner_data.finishing_pos <= 0 ->
        {:error, Error.validation_error("Invalid finishing position", %{position: runner_data.finishing_pos})}
      
      true -> :ok
    end
  end

  @doc """
  Groups transformed runners by their data completeness.
  
  Returns a map with :complete and :incomplete runner lists.
  """
  @spec group_by_completeness([runner_data()]) :: %{complete: [runner_data()], incomplete: [runner_data()]}
  def group_by_completeness(runners) when is_list(runners) do
    Enum.group_by(runners, fn runner ->
      case validate_runner(runner) do
        :ok -> :complete
        {:error, _} -> :incomplete
      end
    end)
    |> Map.put_new(:complete, [])
    |> Map.put_new(:incomplete, [])
  end
end
