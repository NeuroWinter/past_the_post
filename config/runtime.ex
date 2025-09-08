import Config

if config_env() == :dev do
  config :past_the_post, PastThePost.Repo,
    url: System.get_env("DATABASE_URL") || "postgres://postgres:postgres@localhost:5432/past_the_post_dev",
    show_sensitive_data_on_connection_error: true,
    pool_size: 10
end

# Oban (if you set it earlier)
config :past_the_post, Oban,
  repo: PastThePost.Repo,
  queues: [etl: 5]
