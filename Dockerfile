FROM elixir:1.17.2-otp-27-alpine AS build

# Install build dependencies
RUN apk add --no-cache build-base git

WORKDIR /app

# Install hex and rebar
RUN mix local.hex --force && \
    mix local.rebar --force

# Copy mix files
COPY mix.exs mix.lock ./
ENV MIX_ENV=prod
RUN mix deps.get --only prod
RUN mix deps.compile

# Copy source code
COPY . .

# Compile the application
RUN mix compile

# Start runtime stage
FROM elixir:1.17.2-otp-27-alpine AS runtime

RUN apk add --no-cache openssl ncurses-libs

WORKDIR /app

# Copy compiled application
COPY --from=build /app .

ENV MIX_ENV=prod

CMD ["iex", "-S", "mix"]
