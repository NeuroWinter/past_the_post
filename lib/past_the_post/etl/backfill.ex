defmodule PastThePost.ETL.Backfill do
  def enqueue(from_date, to_date) do
    for d <- Date.range(from_date, to_date) do
      Oban.insert!(PastThePost.ETL.BackfillDay.new(%{"date" => Date.to_iso8601(d)}))
    end
    :ok
  end
end

