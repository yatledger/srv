# DAG DB Dockerfile
# Собирает релизный бинарник dagdb и запускает его в минимальном образе.

FROM rust:1.85-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/dagdb /usr/local/bin/dagdb
# genesis.json читается из рабочей директории
COPY genesis.json ./genesis.json
EXPOSE 21001
ENTRYPOINT ["dagdb"]
