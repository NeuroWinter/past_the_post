defmodule PastThePost.ConfigValidator do
  @moduledoc """
  Validates application configuration at startup.
  
  Ensures all required configuration is present and valid before
  the application starts processing data.
  """

  require Logger

  @doc """
  Validates all application configuration.
  
  Should be called during application startup to catch configuration
  issues early.
  """
  @spec validate!() :: :ok | no_return()
  def validate! do
    with :ok <- validate_database_config(),
         :ok <- validate_tab_config(),
         :ok <- validate_oban_config() do
      Logger.info("Configuration validation passed")
      :ok
    else
      {:error, reason} ->
        Logger.error("Configuration validation failed: #{reason}")
        raise "Invalid configuration: #{reason}"
    end
  end

  # Validate database configuration
  defp validate_database_config do
    repo_config = Application.get_env(:past_the_post, PastThePost.Repo)
    
    cond do
      is_nil(repo_config) ->
        {:error, "Missing PastThePost.Repo configuration"}
      
      Mix.env() == :prod and is_nil(System.get_env("DATABASE_URL")) ->
        {:error, "DATABASE_URL environment variable required in production"}
      
      true ->
        :ok
    end
  end

  # Validate TAB API configuration
  defp validate_tab_config do
    tab_config = Application.get_env(:past_the_post, PastThePost.Tab)
    
    cond do
      is_nil(tab_config) ->
        {:error, "Missing PastThePost.Tab configuration"}
      
      is_nil(tab_config[:base_json]) ->
        {:error, "Missing TAB base_json URL"}
      
      not is_integer(tab_config[:rate_ms]) or tab_config[:rate_ms] < 0 ->
        {:error, "Invalid TAB rate_ms setting"}
      
      not is_integer(tab_config[:retries]) or tab_config[:retries] < 0 ->
        {:error, "Invalid TAB retries setting"}
      
      true ->
        validate_tab_url(tab_config[:base_json])
    end
  end

  # Validate TAB URL format
  defp validate_tab_url(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] and not is_nil(host) ->
        :ok
      _ ->
        {:error, "Invalid TAB base_json URL format"}
    end
  end
  defp validate_tab_url(_), do: {:error, "TAB base_json must be a string"}

  # Validate Oban configuration
  defp validate_oban_config do
    oban_config = Application.get_env(:past_the_post, Oban)
    
    cond do
      is_nil(oban_config) ->
        {:error, "Missing Oban configuration"}
      
      oban_config[:repo] != PastThePost.Repo ->
        {:error, "Oban repo must be PastThePost.Repo"}
      
      not is_list(oban_config[:queues]) ->
        {:error, "Oban queues must be a keyword list"}
      
      is_nil(oban_config[:queues][:etl]) ->
        {:error, "Missing Oban ETL queue configuration"}
      
      not is_integer(oban_config[:queues][:etl]) or oban_config[:queues][:etl] <= 0 ->
        {:error, "Oban ETL queue size must be a positive integer"}
      
      true ->
        :ok
    end
  end

  @doc """
  Validates configuration and returns a summary.
  
  Non-raising version that returns detailed information about
  the configuration state.
  """
  @spec validate() :: {:ok, map()} | {:error, [binary()]}
  def validate do
    validations = [
      {"Database", validate_database_config()},
      {"TAB API", validate_tab_config()},
      {"Oban", validate_oban_config()}
    ]

    errors = 
      validations
      |> Enum.filter(fn {_name, result} -> match?({:error, _}, result) end)
      |> Enum.map(fn {name, {:error, reason}} -> "#{name}: #{reason}" end)

    case errors do
      [] ->
        summary = %{
          status: :valid,
          database_url: database_url_summary(),
          tab_config: tab_config_summary(),
          oban_config: oban_config_summary(),
          environment: Mix.env()
        }
        {:ok, summary}
      
      errors ->
        {:error, errors}
    end
  end

  # Get database URL summary (without exposing credentials)
  defp database_url_summary do
    case System.get_env("DATABASE_URL") do
      nil -> "Not set (using config)"
      url ->
        case URI.parse(url) do
          %URI{host: host, port: port, path: path} ->
            "#{host}:#{port}#{path}"
          _ ->
            "Invalid format"
        end
    end
  end

  # Get TAB config summary
  defp tab_config_summary do
    case Application.get_env(:past_the_post, PastThePost.Tab) do
      nil -> "Not configured"
      config ->
        %{
          base_url: config[:base_json],
          rate_limit_ms: config[:rate_ms],
          max_retries: config[:retries]
        }
    end
  end

  # Get Oban config summary
  defp oban_config_summary do
    case Application.get_env(:past_the_post, Oban) do
      nil -> "Not configured"
      config ->
        %{
          queues: config[:queues],
          repo: config[:repo]
        }
    end
  end

  @doc """
  Performs a connectivity test to external services.
  
  Tests database connectivity and TAB API accessibility.
  """
  @spec connectivity_test() :: {:ok, map()} | {:error, map()}
  def connectivity_test do
    results = %{
      database: test_database_connectivity(),
      tab_api: test_tab_api_connectivity()
    }

    case Enum.any?(Map.values(results), &match?({:error, _}, &1)) do
      true -> {:error, results}
      false -> {:ok, results}
    end
  end

  # Test database connectivity
  defp test_database_connectivity do
    try do
      case PastThePost.Repo.query("SELECT 1", []) do
        {:ok, _} -> {:ok, "Connected"}
        {:error, error} -> {:error, "Query failed: #{inspect(error)}"}
      end
    rescue
      error -> {:error, "Connection failed: #{inspect(error)}"}
    end
  end

  # Test TAB API connectivity
  defp test_tab_api_connectivity do
    case PastThePost.ETL.TabClient.health_check() do
      {:ok, _} -> {:ok, "API accessible"}
      {:error, error} -> {:error, "API failed: #{error.message}"}
    end
  end
end
