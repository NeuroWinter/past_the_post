defmodule PastThePost.Repo do
  use Ecto.Repo,
    otp_app: :past_the_post,
    adapter: Ecto.Adapters.Postgres
end
