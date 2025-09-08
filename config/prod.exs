import Config

config :past_the_post, Oban,
  repo: PastThePost.Repo,
  queues: [etl: 10]
