import Config

config :past_the_post, ecto_repos: [PastThePost.Repo]

config :past_the_post, PastThePost.Tab,
  base_json: "https://json.tab.co.nz",
  rate_ms: 350,
  retries: 3

# Oban config (default queue)
config :past_the_post, Oban,
  repo: PastThePost.Repo,
  queues: [etl: 5]

import_config "#{config_env()}.exs"
