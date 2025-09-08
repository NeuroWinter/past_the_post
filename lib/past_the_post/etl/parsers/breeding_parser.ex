defmodule PastThePost.ETL.BreedingParser do
  @moduledoc """
  Parses breeding information from TAB racing data.

  Handles the complex breeding strings that contain age, sex, and parentage
  information in various formats.
  """

  alias PastThePost.ETL.DataParser

  @type breeding_info :: %{
    age: nil | integer(),
    sex: nil | binary(),
    sire_name: nil | binary(),
    dam_name: nil | binary()
  }

  @doc """
  Parses breeding information from TAB format strings.

  ## Examples

      iex> parse("2 f TIZ THE LAW (USA)-CONQUEST STRATE UP (CAN)")
      %{age: 2, sex: "f", sire_name: "TIZ THE LAW", dam_name: "CONQUEST STRATE UP"}

      iex> parse("3 g SUPER STALLION-MARE NAME")
      %{age: 3, sex: "g", sire_name: "SUPER STALLION", dam_name: "MARE NAME"}

      iex> parse(nil)
      %{age: nil, sex: nil, sire_name: nil, dam_name: nil}
  """
  @spec parse(nil | binary()) :: breeding_info()
  def parse(nil), do: empty_breeding_info()
  def parse(breeding_str) when is_binary(breeding_str) do
    breeding_str
    |> String.trim()
    |> parse_breeding_components()
  end

  # Main breeding string pattern: "age sex SIRE-DAM"
  defp parse_breeding_components(breeding_str) do
    case Regex.run(breeding_regex(), breeding_str) do
      [_, age_str, sex, sire_part, dam_part] ->
        %{
          age: DataParser.to_int(age_str),
          sex: normalize_sex(sex),
          sire_name: clean_horse_name(sire_part),
          dam_name: clean_horse_name(dam_part)
        }
      _ ->
        # Fallback: try to extract just sire-dam without age/sex
        parse_simple_pedigree(breeding_str)
    end
  end

  # Fallback parser for simpler pedigree formats
  defp parse_simple_pedigree(breeding_str) do
    case String.split(breeding_str, "-", parts: 2) do
      [sire_part, dam_part] ->
        %{
          age: nil,
          sex: nil,
          sire_name: clean_horse_name(sire_part),
          dam_name: clean_horse_name(dam_part)
        }
      _ ->
        empty_breeding_info()
    end
  end

  # Regex pattern for breeding strings
  # Matches: "2 f TIZ THE LAW (USA)-CONQUEST STRATE UP (CAN)"
  defp breeding_regex do
    ~r/(\d+)\s+([mfgch])\s+(.+?)-(.+)/i
  end

  # Remove country codes and extra whitespace from horse names
  defp clean_horse_name(name_part) when is_binary(name_part) do
    name_part
    |> String.replace(~r/\s*\([A-Z]{2,3}\)\s*$/, "")
    |> String.trim()
    |> DataParser.normalize_string()
  end

  # Normalize sex codes to standard format
  defp normalize_sex(sex) when is_binary(sex) do
    sex
    |> String.downcase()
    |> case do
      s when s in ["m", "male"] -> "m"
      s when s in ["f", "female", "mare"] -> "f"
      s when s in ["g", "gelding"] -> "g"
      s when s in ["c", "colt"] -> "c"
      s when s in ["h", "horse"] -> "h"
      other -> other
    end
  end

  defp empty_breeding_info do
    %{age: nil, sex: nil, sire_name: nil, dam_name: nil}
  end

  @doc """
  Validates breeding information for completeness.

  Returns true if breeding info contains at least sire and dam names.
  """
  @spec valid?(breeding_info()) :: boolean()
  def valid?(%{sire_name: sire, dam_name: dam}) 
    when is_binary(sire) and is_binary(dam) and sire != "" and dam != "", do: true
  def valid?(_), do: false

  @doc """
  Extracts just the parent names from breeding info.

  ## Examples

      iex> extract_parents(%{sire_name: "SIRE", dam_name: "DAM"})
      {"SIRE", "DAM"}

      iex> extract_parents(%{sire_name: nil, dam_name: "DAM"})
      {nil, "DAM"}
  """
  @spec extract_parents(breeding_info()) :: {nil | binary(), nil | binary()}
  def extract_parents(%{sire_name: sire, dam_name: dam}), do: {sire, dam}
end
