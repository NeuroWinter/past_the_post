defmodule PastThePost.ETL.DataParser do
  @moduledoc """
  Common data parsing utilities for ETL operations.
  
  Provides consistent, safe parsing of various data types commonly
  found in racing data feeds.
  """

  @doc """
  Safely converts a value to an integer.
  
  ## Examples
  
      iex> DataParser.to_int("123")
      123
      
      iex> DataParser.to_int(nil)
      nil
      
      iex> DataParser.to_int("invalid")
      nil
  """
  @spec to_int(nil | integer() | binary()) :: nil | integer()
  def to_int(nil), do: nil
  def to_int(value) when is_integer(value), do: value
  def to_int(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {number, _} -> number
      :error -> nil
    end
  end

  @doc """
  Safely converts a value to a float.
  
  ## Examples
  
      iex> DataParser.to_float("123.45")
      123.45
      
      iex> DataParser.to_float(123)
      123.0
      
      iex> DataParser.to_float(nil)
      nil
  """
  @spec to_float(nil | number() | binary()) :: nil | float()
  def to_float(nil), do: nil
  def to_float(value) when is_float(value), do: value
  def to_float(value) when is_integer(value), do: value * 1.0
  def to_float(value) when is_binary(value) do
    case Float.parse(String.trim(value)) do
      {number, _} -> number
      :error -> nil
    end
  end

  @doc """
  Safely trims and normalizes string values.
  
  Returns nil for empty strings or whitespace-only strings.
  
  ## Examples
  
      iex> DataParser.normalize_string("  Hello World  ")
      "Hello World"
      
      iex> DataParser.normalize_string("   ")
      nil
      
      iex> DataParser.normalize_string(nil)
      nil
  """
  @spec normalize_string(nil | binary()) :: nil | binary()
  def normalize_string(nil), do: nil
  def normalize_string(value) when is_binary(value) do
    trimmed = String.trim(value)
    case trimmed do
      "" -> nil
      normalized -> normalized
    end
  end

  @doc """
  Normalizes country codes to uppercase 2-3 character format.
  
  ## Examples
  
      iex> DataParser.normalize_country("nz")
      "NZ"
      
      iex> DataParser.normalize_country("USA")
      "USA"
      
      iex> DataParser.normalize_country(nil)
      "NZ"
  """
  @spec normalize_country(nil | binary()) :: binary()
  def normalize_country(nil), do: "NZ"
  def normalize_country(country) when is_binary(country) do
    country
    |> String.upcase()
    |> String.slice(0, 3)
  end

  @doc """
  Extracts a numeric value from a nested map structure.
  
  Useful for handling inconsistent API response formats.
  
  ## Examples
  
      iex> DataParser.extract_number(%{"distance" => "1200"}, ["distance", "distanceMeters"])
      1200
      
      iex> DataParser.extract_number(%{"distanceMeters" => 1200}, ["distance", "distanceMeters"])
      1200
  """
  @spec extract_number(map(), [binary()]) :: nil | integer()
  def extract_number(data, keys) when is_map(data) and is_list(keys) do
    keys
    |> Enum.find_value(fn key -> Map.get(data, key) end)
    |> to_int()
  end

  @doc """
  Extracts a string value from a nested map structure.
  
  ## Examples
  
      iex> DataParser.extract_string(%{"venue" => "Ellerslie"}, ["venue", "name"])
      "Ellerslie"
  """
  @spec extract_string(map(), [binary()]) :: nil | binary()
  def extract_string(data, keys) when is_map(data) and is_list(keys) do
    keys
    |> Enum.find_value(fn key -> Map.get(data, key) end)
    |> normalize_string()
  end
end
