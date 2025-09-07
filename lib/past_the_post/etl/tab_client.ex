defmodule PastThePost.ETL.TabClient do
  @moduledoc "Thin client for TAB NZ JSON feeds."
  @base Application.compile_env!(:past_the_post, PastThePost.Tab)[:base_json]
  @rate Application.compile_env!(:past_the_post, PastThePost.Tab)[:rate_ms]
  @retries Application.compile_env!(:past_the_post, PastThePost.Tab)[:retries]

  def schedule!(date), do: get!("#{@base}/schedule/#{Date.to_iso8601(date)}")
  def results_for_meeting!(date, meetno), do: get!("#{@base}/results/#{Date.to_iso8601(date)}/#{meetno}")

  defp get!(url) do
    resp =
      Req.get!(url,
        retry: :unsafe,
        max_retries: @retries,
        retry_delay: fn a -> trunc(:math.pow(2, a) * 100) end,
        receive_timeout: 15_000
      )
    Process.sleep(@rate)
    resp.body
  end
end

