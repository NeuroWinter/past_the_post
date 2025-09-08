defmodule PastThePost.ETL.TabClient do
  @moduledoc """
  Enhanced client for TAB NZ JSON feeds with improved error handling and monitoring.
  
  Provides robust API access with structured error handling, rate limiting compliance,
  and detailed logging for debugging and monitoring.
  """

  require Logger
  alias PastThePost.ETL.Error

  @base_url Application.compile_env!(:past_the_post, PastThePost.Tab)[:base_json]
  @rate_limit_ms Application.compile_env!(:past_the_post, PastThePost.Tab)[:rate_ms]
  @max_retries Application.compile_env!(:past_the_post, PastThePost.Tab)[:retries]
  @request_timeout 15_000

  @type api_response :: {:ok, map()} | {:error, Error.t()}

  @doc """
  Fetches the racing schedule for a specific date.
  
  ## Examples
  
      iex> schedule(~D[2024-01-01])
      {:ok, %{"meetings" => [...]}}
      
      iex> schedule(~D[2024-01-01])
      {:error, %Error{type: :api_error, ...}}
  """
  @spec schedule(Date.t()) :: api_response()
  def schedule(date) do
    url = "#{@base_url}/schedule/#{Date.to_iso8601(date)}"
    
    with {:ok, response} <- make_request(url, %{operation: "schedule", date: date}) do
      validate_schedule_response(response)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Fetches results for a specific meeting.
  
  ## Examples
  
      iex> results_for_meeting(~D[2024-01-01], 1)
      {:ok, %{"meetings" => [...]}}
  """
  @spec results_for_meeting(Date.t(), integer() | binary()) :: api_response()
  def results_for_meeting(date, meetno) do
    url = "#{@base_url}/results/#{Date.to_iso8601(date)}/#{meetno}"
    
    context = %{operation: "results", date: date, meeting_number: meetno}
    
    with {:ok, response} <- make_request(url, context) do
      validate_results_response(response)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Legacy function for backward compatibility.
  
  Raises on error - use `schedule/1` for better error handling.
  """
  @spec schedule!(Date.t()) :: map()
  def schedule!(date) do
    case schedule(date) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  @doc """
  Legacy function for backward compatibility.
  
  Raises on error - use `results_for_meeting/2` for better error handling.
  """
  @spec results_for_meeting!(Date.t(), integer() | binary()) :: map()
  def results_for_meeting!(date, meetno) do
    case results_for_meeting(date, meetno) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  # Private functions

  # Makes HTTP request with retry logic and error handling
  defp make_request(url, context) do
    start_time = System.monotonic_time(:millisecond)
    
    Logger.debug("Making TAB API request", Map.put(context, :url, url))
    
    try do
      response = Req.get!(url,
        retry: :transient,
        retry_log_level: :warn,
        max_retries: @max_retries,
        retry_delay: &exponential_backoff/1,
        receive_timeout: @request_timeout,
        headers: [
          {"user-agent", "past_the_post/0.1 (+https://github.com/yourorg/past_the_post)"},
          {"accept", "application/json"}
        ]
      )

      # Respect rate limiting
      Process.sleep(@rate_limit_ms)
      
      end_time = System.monotonic_time(:millisecond)
      duration = end_time - start_time
      
      case response.status do
        200 ->
          Logger.debug("TAB API request successful", 
            Map.merge(context, %{status: 200, duration_ms: duration}))
          {:ok, response.body}
        
        404 ->
          error = Error.api_error("Resource not found", 
            Map.merge(context, %{status: 404, url: url}))
          Logger.warning("TAB API resource not found", 
            error_context: Error.format_for_logging(error))
          {:error, error}
        
        429 ->
          retry_after = extract_retry_after(response.headers)
          error = Error.rate_limit_error("Rate limit exceeded", retry_after, 
            Map.merge(context, %{status: 429, url: url}))
          Logger.warning("TAB API rate limit exceeded", 
            error_context: Error.format_for_logging(error))
          {:error, error}
        
        status when status >= 500 ->
          error = Error.api_error("Server error", 
            Map.merge(context, %{status: status, url: url}))
          Logger.error("TAB API server error", 
            error_context: Error.format_for_logging(error))
          {:error, error}
        
        status ->
          error = Error.api_error("Unexpected status code", 
            Map.merge(context, %{status: status, url: url}))
          Logger.error("TAB API unexpected status", 
            error_context: Error.format_for_logging(error))
          {:error, error}
      end
    rescue
      exception in [Req.TransportError] ->
        reason = Map.get(exception, :reason, "unknown")
        error = Error.network_error("Network error: #{inspect(reason)}", 
          Map.merge(context, %{url: url, transport_error: reason}))
        Logger.error("TAB API network error", 
          error_context: Error.format_for_logging(error))
        {:error, error}
      
      exception in [Req.HTTPError] ->
        error = Error.api_error("HTTP error: #{Exception.message(exception)}", 
          Map.merge(context, %{url: url, http_error: inspect(exception)}))
        Logger.error("TAB API HTTP error", 
          error_context: Error.format_for_logging(error))
        {:error, error}
      
      exception ->
        error = Error.api_error("Request failed: #{Exception.message(exception)}", 
          Map.merge(context, %{url: url, exception: inspect(exception)}))
        Logger.error("TAB API request exception", 
          error_context: Error.format_for_logging(error))
        {:error, error}
    end
  end

  # Validates schedule API response structure
  defp validate_schedule_response(response) when is_map(response) do
    case response do
      %{"meetings" => meetings} when is_list(meetings) ->
        {:ok, response}
      
      %{"meetings" => _} ->
        {:error, Error.parse_error("Invalid meetings format in schedule response", %{response: response})}
      
      _ ->
        {:error, Error.parse_error("Missing meetings field in schedule response", %{response: response})}
    end
  end
  defp validate_schedule_response(response) do
    {:error, Error.parse_error("Schedule response is not a map", %{response: response})}
  end

  # Validates results API response structure
  defp validate_results_response(response) when is_map(response) do
    case response do
      %{"meetings" => meetings} when is_list(meetings) ->
        {:ok, response}
      
      %{"meetings" => _} ->
        {:error, Error.parse_error("Invalid meetings format in results response", %{response: response})}
      
      # Some results endpoints might return different structures
      %{} ->
        Logger.warning("Results response missing meetings field, assuming empty", %{response: response})
        {:ok, %{"meetings" => []}}
    end
  end
  defp validate_results_response(response) do
    {:error, Error.parse_error("Results response is not a map", %{response: response})}
  end

  # Exponential backoff with jitter for retry delays
  defp exponential_backoff(attempt) do
    base_delay = 200 # Start with 200ms
    max_delay = 10_000 # Cap at 10 seconds
    jitter = :rand.uniform(500) # Add up to 500ms jitter
    
    delay = min(base_delay * :math.pow(2, attempt) + jitter, max_delay)
    trunc(delay)
  end

  # Extract retry-after header value
  defp extract_retry_after(headers) do
    case Enum.find(headers, fn {name, _value} -> 
      String.downcase(name) == "retry-after" 
    end) do
      {_name, value} ->
        case Integer.parse(value) do
          {seconds, _} -> seconds
          :error -> 60 # Default to 60 seconds
        end
      nil -> 60 # Default to 60 seconds
    end
  end

  @doc """
  Gets API client health status.
  
  Returns information about recent request patterns and any issues.
  """
  @spec health_check() :: {:ok, map()} | {:error, Error.t()}
  def health_check do
    test_date = Date.utc_today()
    
    case schedule(test_date) do
      {:ok, _response} ->
        {:ok, %{
          status: :healthy,
          base_url: @base_url,
          rate_limit_ms: @rate_limit_ms,
          max_retries: @max_retries,
          last_check: DateTime.utc_now()
        }}
      
      {:error, error} ->
        {:error, Error.api_error("Health check failed", %{
          base_url: @base_url,
          test_date: test_date,
          underlying_error: Error.format_for_logging(error)
        })}
    end
  end
end
