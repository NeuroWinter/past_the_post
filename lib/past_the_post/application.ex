defmodule PastThePost.Application do
  use Application
  require Logger

  def start(_type, _args) do
    # Validate configuration before starting any processes
    PastThePost.ConfigValidator.validate!()

    children = [
      PastThePost.Repo,
      {Oban, Application.fetch_env!(:past_the_post, Oban)}
    ]

    opts = [strategy: :one_for_one, name: PastThePost.Supervisor]
    
    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        log_startup_success()
        {:ok, pid}
      
      {:error, reason} ->
        Logger.error("Application failed to start: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Optional: Add a startup health check
  defp log_startup_success do
    case PastThePost.ConfigValidator.connectivity_test() do
      {:ok, results} ->
        Logger.info("Application started successfully", %{
          connectivity: results,
          environment: Mix.env()
        })
      
      {:error, results} ->
        Logger.warning("Application started but connectivity issues detected", %{
          connectivity: results,
          environment: Mix.env()
        })
    end
  end
end
