defmodule PastThePost.ETL.Error do
  @moduledoc """
  Structured error handling for ETL operations.
  
  Provides consistent error types and context for debugging
  and monitoring ETL pipeline issues.
  """

  defexception [:type, :message, :context, :retry_after]

  @type error_type :: 
    :api_error |
    :parse_error |
    :validation_error |
    :database_error |
    :rate_limit_error |
    :network_error

  @type t :: %__MODULE__{
    type: error_type(),
    message: String.t(),
    context: map(),
    retry_after: nil | non_neg_integer()
  }

  @doc """
  Creates an API error with context.
  
  ## Examples
  
      iex> ETLError.api_error("Failed to fetch schedule", %{date: ~D[2024-01-01], status: 404})
      %ETLError{type: :api_error, message: "Failed to fetch schedule", context: %{date: ~D[2024-01-01], status: 404}}
  """
  @spec api_error(String.t(), map(), nil | non_neg_integer()) :: t()
  def api_error(message, context \\ %{}, retry_after \\ nil) do
    %__MODULE__{
      type: :api_error,
      message: message,
      context: context,
      retry_after: retry_after
    }
  end

  @doc """
  Creates a parse error with context about the problematic data.
  """
  @spec parse_error(String.t(), map()) :: t()
  def parse_error(message, context \\ %{}) do
    %__MODULE__{
      type: :parse_error,
      message: message,
      context: context,
      retry_after: nil
    }
  end

  @doc """
  Creates a validation error with details about what failed validation.
  """
  @spec validation_error(String.t(), map()) :: t()
  def validation_error(message, context \\ %{}) do
    %__MODULE__{
      type: :validation_error,
      message: message,
      context: context,
      retry_after: nil
    }
  end

  @doc """
  Creates a database error with query context.
  """
  @spec database_error(String.t(), map()) :: t()
  def database_error(message, context \\ %{}) do
    %__MODULE__{
      type: :database_error,
      message: message,
      context: context,
      retry_after: 30
    }
  end

  @doc """
  Creates a rate limit error with retry timing.
  """
  @spec rate_limit_error(String.t(), non_neg_integer(), map()) :: t()
  def rate_limit_error(message, retry_after, context \\ %{}) do
    %__MODULE__{
      type: :rate_limit_error,
      message: message,
      context: context,
      retry_after: retry_after
    }
  end

  @doc """
  Creates a network error for connection issues.
  """
  @spec network_error(String.t(), map()) :: t()
  def network_error(message, context \\ %{}) do
    %__MODULE__{
      type: :network_error,
      message: message,
      context: context,
      retry_after: 60
    }
  end

  @doc """
  Determines if an error is retryable based on its type.
  """
  @spec retryable?(t()) :: boolean()
  def retryable?(%__MODULE__{type: type}) do
    type in [:api_error, :network_error, :database_error, :rate_limit_error]
  end

  @doc """
  Gets the retry delay for an error, with exponential backoff.
  """
  @spec retry_delay(t(), non_neg_integer()) :: non_neg_integer()
  def retry_delay(%__MODULE__{retry_after: retry_after}, attempt) when is_integer(retry_after) do
    base_delay = retry_after * 1000 # Convert to milliseconds
    jitter = :rand.uniform(1000) # Add up to 1 second jitter
    
    min(base_delay * :math.pow(2, attempt) + jitter, 300_000) # Max 5 minutes
    |> trunc()
  end
  def retry_delay(_error, _attempt), do: 0

  @doc """
  Formats error for logging with structured context.
  """
  @spec format_for_logging(t()) :: map()
  def format_for_logging(%__MODULE__{} = error) do
    %{
      error_type: error.type,
      error_message: error.message,
      error_context: error.context,
      retry_after: error.retry_after,
      retryable: retryable?(error)
    }
  end

  # Exception protocol implementation
  def message(%__MODULE__{type: type, message: message, context: context}) do
    base_message = "[#{type}] #{message}"
    
    case context do
      context when map_size(context) == 0 -> base_message
      context -> "#{base_message} | Context: #{inspect(context)}"
    end
  end
end
