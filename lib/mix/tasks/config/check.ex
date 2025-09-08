defmodule Mix.Tasks.Config.Check do
  @moduledoc """
  Validates PastThePost configuration and optionally tests connectivity.

  ## Examples

      mix config.check
      mix config.check --connectivity
      mix config.check -c
  """
  
  use Mix.Task

  @shortdoc "Validates application configuration"

  def run(args) do
    # Start the application configuration
    Mix.Task.run("app.config")

    case parse_args(args) do
      %{connectivity: true} ->
        run_full_validation()
      
      %{connectivity: false} ->
        run_config_validation()
    end
  end

  defp parse_args(args) do
    {opts, _remaining_args, _invalid} = OptionParser.parse(args, 
      switches: [connectivity: :boolean],
      aliases: [c: :connectivity]
    )
    
    %{connectivity: Keyword.get(opts, :connectivity, false)}
  end

  defp run_config_validation do
    case PastThePost.ConfigValidator.validate() do
      {:ok, summary} ->
        Mix.shell().info("✅ Configuration validation passed")
        print_summary(summary)
        
      {:error, errors} ->
        Mix.shell().error("❌ Configuration validation failed:")
        Enum.each(errors, fn error ->
          Mix.shell().error("  • #{error}")
        end)
        System.halt(1)
    end
  end

  defp run_full_validation do
    # First validate config
    run_config_validation()

    # Then test connectivity
    Mix.shell().info("\n🔌 Testing connectivity...")
    
    # Start minimal application for connectivity test
    Application.ensure_all_started(:past_the_post)
    
    case PastThePost.ConfigValidator.connectivity_test() do
      {:ok, results} ->
        Mix.shell().info("✅ Connectivity test passed")
        print_connectivity_results(results)
        
      {:error, results} ->
        Mix.shell().error("❌ Connectivity test failed:")
        print_connectivity_results(results)
        System.halt(1)
    end
  end

  defp print_summary(summary) do
    Mix.shell().info("\n📋 Configuration Summary:")
    Mix.shell().info("  Environment: #{summary.environment}")
    Mix.shell().info("  Database: #{summary.database_url}")
    
    if is_map(summary.tab_config) do
      Mix.shell().info("  TAB API: #{summary.tab_config.base_url}")
      Mix.shell().info("  Rate limit: #{summary.tab_config.rate_limit_ms}ms")
    end
    
    if is_map(summary.oban_config) do
      queues = Enum.map(summary.oban_config.queues, fn {name, size} -> "#{name}:#{size}" end)
      Mix.shell().info("  Oban queues: #{Enum.join(queues, ", ")}")
    end
  end

  defp print_connectivity_results(results) do
    Enum.each(results, fn {service, result} ->
      case result do
        {:ok, message} ->
          Mix.shell().info("  ✅ #{service}: #{message}")
        {:error, message} ->
          Mix.shell().error("  ❌ #{service}: #{message}")
      end
    end)
  end
end
