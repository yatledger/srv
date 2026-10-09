# DAG DB Dockerfile
# Собирает релизный бинарник dagdb и запускает его в минимальном образе.

FROM rust:1.99-slim AS builder
WORKDIR /app
# redis/reqwest через native-tls требуют OpenSSL заголовков при сборке.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/dagdb /usr/local/bin/dagdb
# genesis.json читается из рабочей директории
COPY genesis.json ./genesis.json
EXPOSE 21001 21002 21003 21004
ENTRYPOINT ["dagdb"]
