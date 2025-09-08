import Config

if config_env() == :dev do
  config :past_the_post, PastThePost.Repo,
    url: System.get_env("DATABASE_URL") || "postgres://postgres:postgres@localhost:5432/past_the_post_dev",
    show_sensitive_data_on_connection_error: true,
    pool_size: 10
end

if config_env() == :prod do
  # Temporary hardcode - replace with env var later
  config :past_the_post, PastThePost.Repo,
    adapter: Ecto.Adapters.Postgres,
    url: System.get_env("DATABASE_URL") || raise "DATABASE_URL environment variable is missing.",
    pool_size: 10,
    ssl: true,
    ssl_opts: [verify: :verify_none]
end

config :past_the_post, Oban,
  repo: PastThePost.Repo,
  queues: [etl: 5]
