defmodule PastThePost.ETL.BackfillDay do
  use Oban.Worker, queue: :etl, max_attempts: 5

  import Ecto.Query, only: [from: 2]

  alias PastThePost.Repo
  alias PastThePost.ETL.TabClient
  alias PastThePost.Racing.{Race, Entry, Trainer, Jockey}
  alias PastThePost.Blood.Horse
  require Logger

  # -------- helpers --------
  defp to_int(nil), do: nil
  defp to_int(v) when is_integer(v), do: v
  defp to_int(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, _} -> n
      :error -> nil
    end
  end

  defp to_float(nil), do: nil
  defp to_float(v) when is_float(v), do: v
  defp to_float(v) when is_integer(v), do: v * 1.0
  defp to_float(v) when is_binary(v) do
    case Float.parse(v) do
      {n, _} -> n
      :error -> nil
    end
  end

  # Parse breeding information from TAB format
  # Example: "2 f TIZ THE LAW (USA)-CONQUEST STRATE UP (CAN)"
  defp parse_breeding_info(nil), do: {nil, nil, nil, nil}
  defp parse_breeding_info(breeding_str) when is_binary(breeding_str) do
    case Regex.run(~r/(\d+)\s+([mfgch])\s+(.+)-(.+)/, String.trim(breeding_str)) do
      [_, age_str, sex, sire_part, dam_part] ->
        age = to_int(age_str)
        sire_name = sire_part |> String.replace(~r/\s+\([A-Z]+\)$/, "") |> String.trim()
        dam_name = dam_part |> String.replace(~r/\s+\([A-Z]+\)$/, "") |> String.trim()
        {age, sex, sire_name, dam_name}
      _ -> {nil, nil, nil, nil}
    end
  end

  # Upsert horse and optionally create parent relationships
  defp upsert_horse_with_bloodline(horse_attrs, sire_name \\ nil, dam_name \\ nil) do
    # First, ensure parent horses exist if provided
    sire = if sire_name, do: maybe_upsert_horse(sire_name), else: nil
    dam = if dam_name, do: maybe_upsert_horse(dam_name), else: nil

    # Now upsert the main horse with parent references
    horse_attrs_with_parents = 
      horse_attrs
      |> Map.put(:sire_id, sire && sire.id)
      |> Map.put(:dam_id, dam && dam.id)

    horse = Repo.insert!(
      struct(Horse, horse_attrs_with_parents),
      on_conflict: {:replace, [:country, :year_foaled, :sex, :sire_id, :dam_id]},
      conflict_target: [:name]
    )

    Repo.get_by!(Horse, name: horse_attrs.name)
  end

  # Simple horse upsert for parent horses
  defp maybe_upsert_horse(name) when is_binary(name) and name != "" do
    Repo.insert!(
      %Horse{name: String.trim(name)},
      on_conflict: :nothing,
      conflict_target: [:name]
    )
    Repo.get_by!(Horse, name: String.trim(name))
  end
  defp maybe_upsert_horse(_), do: nil

  defp maybe_upsert_name(_schema, nil), do: nil
  defp maybe_upsert_name(_schema, ""), do: nil
  defp maybe_upsert_name(schema, name) when is_binary(name) do
    trimmed_name = String.trim(name)
    if trimmed_name != "" do
      Repo.insert!(
        struct(schema, %{name: trimmed_name}),
        on_conflict: :nothing,
        conflict_target: [:name]
      )
      Repo.get_by!(schema, name: trimmed_name)
    else
      nil
    end
  end

  # -------- results normalizer (TAB finals) --------
  # Turns placings/also_ran into "runner"-like maps the rest of the pipeline understands.
  defp normalize_runners_from_results(race_map) do
    placings = race_map["placings"] || []
    also_ran = race_map["also_ran"] || []

    placed =
      Enum.map(placings, fn p ->
        %{
          "horse"      => %{"name" => p["name"]},
          "jockey"     => p["jockey"],
          "placing"    => p["rank"],               # integer 1..N
          "margin"     => p["distance"],           # beaten distance (may be string)
          "barrier"    => nil,
          "weight"     => nil,
          "trainer"    => nil,
          "fixedOdds"  => nil,
          "sp"         => nil,
          "betfairSP"  => nil
        }
      end)

    losers =
      Enum.map(also_ran, fn a ->
        fp = a["finish_position"]
        %{
          "horse"      => %{"name" => a["name"]},
          "jockey"     => a["jockey"],
          "placing"    => (if fp == 0, do: nil, else: fp),
          "margin"     => a["distance"],
          "barrier"    => a["barrier"],
          "weight"     => a["weight"],
          "trainer"    => nil,
          "fixedOdds"  => nil,
          "sp"         => nil,
          "betfairSP"  => nil
        }
      end)

    placed ++ losers
  end

  # -------- Oban entry --------
  @impl true
  def perform(%Oban.Job{args: %{"date" => iso}}) do
    date = Date.from_iso8601!(iso)
    sched = TabClient.schedule!(date)

    Enum.each(sched["meetings"] || [], fn m ->
      meetno = m["number"] || m["meetNo"] || m["meetno"]

      results =
        try do
          TabClient.results_for_meeting!(date, meetno)
        rescue
          error ->
            Logger.warning("Failed to fetch results for meeting #{meetno} on #{date}: #{inspect(error)}")
            nil
        end

      upsert_meeting_results!(date, m, results)
    end)

    :ok
  end

  # -------- normalizer + core --------
  defp upsert_meeting_results!(date, meeting, results_or_nil) do
    races =
      cond do
        # Handle results API response (nested under meetings)
        is_map(results_or_nil) and Map.has_key?(results_or_nil, "meetings") ->
          results_meetings = results_or_nil["meetings"] || []
          # Find the matching meeting by number
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

    do_upsert_meeting_results!(date, meeting, races)
  end

  defp do_upsert_meeting_results!(_date, _meeting, []), do: :ok

  defp do_upsert_meeting_results!(date, meeting, races) when is_list(races) do
    track = meeting["venue"] || meeting["name"] || "Unknown"
    country = (meeting["country"] || "NZ") |> to_string() |> String.upcase() |> String.slice(0, 2)

    Enum.each(races, fn r ->
      distance = to_int(r["distance"] || r["distanceMeters"] || r["length"]) || 0
      going = r["trackCondition"] || r["going"] || r["track"]
      race_no = to_int(r["number"]) || 0

      race =
        Repo.insert!(%Race{
          date: date,
          track: track,
          country: country,
          distance_m: distance,
          going: going,
          class: r["class"] || r["raceClass"],
          race_number: race_no
        },
        on_conflict: {:replace, [:distance_m, :going, :class]},
        conflict_target: [:date, :track, :race_number])

      # Extract breeding information for winner (if available)
      {winner_age, winner_sex, sire_name, dam_name} = parse_breeding_info(r["winnersbreeding"])

      runners =
        cond do
          is_list(r["results"]) && r["results"] != [] -> r["results"]
          is_list(r["runners"]) && r["runners"] != [] -> r["runners"]
          is_list(r["entries"]) && r["entries"] != [] -> r["entries"]
          (is_list(r["placings"]) && r["placings"] != []) or
            (is_list(r["also_ran"]) && r["also_ran"] != []) ->
            normalize_runners_from_results(r)
          true -> []
        end

      Enum.each(runners, fn runner ->
        horse_map = runner["horse"] || runner
        horse_name = horse_map["name"]

        # Skip obviously bad rows
        if is_binary(horse_name) and String.trim(horse_name) != "" do
          # Check if this is the winner and we have breeding info
          is_winner = runner["placing"] == 1 || runner["finishPosition"] == 1 || runner["rank"] == 1
          
          horse = 
            if is_winner && sire_name && dam_name do
              # Winner with breeding info - create with bloodline
              horse_attrs = %{
                name: horse_name,
                country: horse_map["country"],
                year_foaled: to_int(horse_map["yob"]),
                sex: winner_sex
              }
              upsert_horse_with_bloodline(horse_attrs, sire_name, dam_name)
            else
              # Regular horse upsert
              Repo.insert!(%Horse{
                name: horse_name,
                country: horse_map["country"],
                year_foaled: to_int(horse_map["yob"])
              }, on_conflict: :nothing, conflict_target: [:name])
              
              Repo.get_by!(Horse, name: horse_name)
            end

          trainer = maybe_upsert_name(Trainer, runner["trainer"])
          jockey = maybe_upsert_name(Jockey, runner["jockey"])

          Repo.insert!(
            %Entry{
              race_id: race.id,
              horse_id: horse.id,
              trainer_id: trainer && trainer.id,
              jockey_id: jockey && jockey.id,
              barrier: to_int(runner["barrier"]),
              weight_kg: to_float(runner["weight"]),
              finishing_pos: to_int(runner["placing"] || runner["finishPosition"]),
              margin_l: to_float(runner["margin"]),
              sp_odds: to_float(runner["fixedOdds"] || runner["sp"]),
              bf_sp: to_float(runner["betfairSP"])
            },
            on_conflict: (
              from e in Entry,
                update: [
                  set: [
                    trainer_id: fragment("COALESCE(EXCLUDED.trainer_id, ?)", e.trainer_id),
                    jockey_id: fragment("COALESCE(EXCLUDED.jockey_id, ?)", e.jockey_id),
                    barrier: fragment("COALESCE(EXCLUDED.barrier, ?)", e.barrier),
                    weight_kg: fragment("COALESCE(EXCLUDED.weight_kg, ?)", e.weight_kg),
                    finishing_pos: fragment("COALESCE(EXCLUDED.finishing_pos, ?)", e.finishing_pos),
                    margin_l: fragment("COALESCE(EXCLUDED.margin_l, ?)", e.margin_l),
                    sp_odds: fragment("COALESCE(EXCLUDED.sp_odds, ?)", e.sp_odds),
                    bf_sp: fragment("COALESCE(EXCLUDED.bf_sp, ?)", e.bf_sp)
                  ]
                ]
            ),
            conflict_target: [:race_id, :horse_id]
          )
        end
      end)
    end)

    :ok
  end
end
