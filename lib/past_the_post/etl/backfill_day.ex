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

  # Upsert and ensure we have an ID (useful with on_conflict: :nothing)
  defp upsert_get(schema, attrs, conflict \\ [name: :name]) do
    Repo.insert!(struct(schema, attrs), on_conflict: :nothing)
    # conflict is {field_key, param_key_atom} or list with one pair
    {field, param} =
      case conflict do
        [{f, p}] -> {f, p}
        {f, p} -> {f, p}
        f when is_atom(f) -> {f, f}
      end

    Repo.get_by!(schema, [{field, Map.get(attrs, param)}])
  end

  defp maybe_upsert_name(_schema, nil), do: nil
  defp maybe_upsert_name(schema, name) do
    Repo.insert!(
      struct(schema, %{name: name}),
      on_conflict: :nothing,
      conflict_target: [:name]      # <— rely on the unique index
    )

    Repo.get_by!(schema, name: name)
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
    date  = Date.from_iso8601!(iso)
    sched = TabClient.schedule!(date)

    Enum.each(sched["meetings"] || [], fn m ->
      meetno = m["number"] || m["meetNo"] || m["meetno"]

      results =
        try do
          TabClient.results_for_meeting!(date, meetno)
        rescue
          _ -> nil
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
    track   = meeting["venue"] || meeting["name"] || "Unknown"
    country = (meeting["country"] || "NZ") |> to_string() |> String.upcase() |> String.slice(0, 2)

    Enum.each(races, fn r ->
      distance = to_int(r["distance"] || r["distanceMeters"] || r["length"]) || 0
      going    = r["trackCondition"] || r["going"] || r["track"]
      race_no  = to_int(r["number"]) || 0

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
          horse =
            Repo.insert!(%Horse{
              name: horse_map["name"],
              country: horse_map["country"],
              year_foaled: to_int(horse_map["yob"])
            }, on_conflict: :nothing, conflict_target: [:name])

          horse = Repo.get_by!(Horse, name: horse_map["name"])
          trainer = maybe_upsert_name(Trainer, runner["trainer"])
          jockey  = maybe_upsert_name(Jockey,  runner["jockey"])

          Repo.insert!(
            %Entry{
              race_id: race.id,
              horse_id: horse.id,
              trainer_id: trainer && trainer.id,
              jockey_id:  jockey  && jockey.id,
              barrier: to_int(runner["barrier"]),
              weight_kg: to_float(runner["weight"]),
              finishing_pos: to_int(runner["placing"] || runner["finishPosition"]),
              margin_l: to_float(runner["margin"]),
              sp_odds:  to_float(runner["fixedOdds"] || runner["sp"]),
              bf_sp:    to_float(runner["betfairSP"])
            },
            on_conflict: (
              from e in Entry,
                update: [
                  set: [
                    trainer_id: fragment("COALESCE(EXCLUDED.trainer_id, ?)", e.trainer_id),
                    jockey_id:  fragment("COALESCE(EXCLUDED.jockey_id,  ?)", e.jockey_id),
                    barrier:    fragment("COALESCE(EXCLUDED.barrier,    ?)", e.barrier),
                    weight_kg:  fragment("COALESCE(EXCLUDED.weight_kg,  ?)", e.weight_kg),

                    finishing_pos: fragment("COALESCE(EXCLUDED.finishing_pos, ?)", e.finishing_pos),
                    margin_l:      fragment("COALESCE(EXCLUDED.margin_l,      ?)", e.margin_l),
                    sp_odds:       fragment("COALESCE(EXCLUDED.sp_odds,       ?)", e.sp_odds),
                    bf_sp:         fragment("COALESCE(EXCLUDED.bf_sp,         ?)", e.bf_sp)
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
