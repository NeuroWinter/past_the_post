defmodule PastThePost.ETL.BackfillDay do
  use Oban.Worker, queue: :etl, max_attempts: 5
  alias PastThePost.{Repo}
  alias PastThePost.ETL.TabClient
  alias PastThePost.Racing.{Race, Entry, Trainer, Jockey}
  alias PastThePost.Blood.Horse

  @impl true
  def perform(%Oban.Job{args: %{"date" => iso}}) do
    date = Date.from_iso8601!(iso)
    sched = TabClient.schedule!(date)

    for m <- (sched["meetings"] || []) do
      meetno = m["number"] || m["meetNo"] || m["meetno"]
      results = TabClient.results_for_meeting!(date, meetno)
      upsert_meeting_results!(date, m, results)
    end

    :ok
  end

  defp upsert_meeting_results!(date, meeting, %{"races" => races}) do
    track   = meeting["venue"] || meeting["name"]
    country = meeting["country"] || "NZ"

    Enum.each(races, fn r ->
      race =
        Repo.insert!(
          %Race{
            date: date,
            track: track,
            country: country,
            distance_m: r["distance"] || r["distanceMeters"],
            going: r["trackCondition"] || r["going"],
            class: r["class"] || r["raceClass"],
            race_number: r["number"]
          },
          on_conflict: {:replace, [:distance_m, :going, :class]},
          conflict_target: [:date, :track, :race_number]
        )

      for runner <- (r["results"] || r["runners"] || []) do
        h = runner["horse"] || %{}
        horse =
          Repo.insert!(%Horse{name: h["name"], country: h["country"], year_foaled: h["yob"]},
            on_conflict: :nothing
          )

        trainer = maybe_upsert_name(Trainer, runner["trainer"])
        jockey  = maybe_upsert_name(Jockey, runner["jockey"])

        Repo.insert!(
          %Entry{
            race_id: race.id,
            horse_id: horse.id,
            trainer_id: trainer && trainer.id,
            jockey_id: jockey && jockey.id,
            finishing_pos: runner["placing"] || runner["finishPosition"],
            margin_l: runner["margin"],
            sp_odds: runner["fixedOdds"] || runner["sp"],
            bf_sp: runner["betfairSP"]
          },
          on_conflict: {:replace, [:trainer_id, :jockey_id, :finishing_pos, :margin_l, :sp_odds, :bf_sp]},
          conflict_target: [:race_id, :horse_id]
        )
      end
    end)
  end

  defp maybe_upsert_name(_schema, nil), do: nil
  defp maybe_upsert_name(schema, name) do
    Repo.insert!(struct(schema, %{name: name}), on_conflict: :nothing)
    Repo.get_by!(schema, name: name)
  end
end

