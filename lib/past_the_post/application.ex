defmodule PastThePost.Application do
  use Application

  def start(_type, _args) do
    children = [
      PastThePost.Repo,
      {Oban, Application.fetch_env!(:past_the_post, Oban)}
    ]

    opts = [strategy: :one_for_one, name: PastThePost.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

