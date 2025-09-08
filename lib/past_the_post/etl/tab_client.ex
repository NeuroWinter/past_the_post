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
        retry: :transient,               # <- was :unsafe
        retry_log_level: :warn,
        max_retries: @retries,
        retry_delay: fn attempt -> trunc(:math.pow(2, attempt) * 200) end,
        receive_timeout: 15_000,
        headers: [{"user-agent", "past_the_post/0.1 (+https://github.com/yourname/past_the_post)"}]
      )

    Process.sleep(@rate)
    resp.body
  end
end
