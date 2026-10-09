#!/usr/bin/env bash
#
# Обёртка нагрузочного генератора dagdb.
#
# Собирает release-бинарь `loadgen`, обеспечивает доступность Redis (при
# необходимости поднимает контейнер) и запускает нагрузочный сценарий на
# in-process кластере. Переданные аргументы заменяют набор по умолчанию
# (а не дополняют его), чтобы не плодить конфликтующие флаги.
#
# Единственный источник подключения к Redis — `REDIS_URL`: сначала из
# окружения, иначе из файла `.env`. Пароль для контейнера берётся из userinfo
# этой строки (`redis://[user]:password@host:port/db`) — ничего не выдумываем.
#
# Переменные окружения:
#   REDIS_URL        строка подключения (по умолчанию redis://127.0.0.1:6379/0)
#   REDIS_CONTAINER  имя docker-контейнера (по умолчанию dagdb-redis)
#
# Примеры:
#   ./scripts/loadgen.sh
#   ./scripts/loadgen.sh --tx 2000 --tps 1000 --parents 3
#
set -euo pipefail

cd "$(dirname "$0")/.."

REDIS_CONTAINER="${REDIS_CONTAINER:-dagdb-redis}"

# Читает значение переменной `key` из `.env` (приоритет — у окружения процесса).
read_env_var() {
    local key="$1"
    [[ -f .env ]] || return 1
    local line
    line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" .env | tail -n1 || true)"
    [[ -n "$line" ]] || return 1
    local value="${line#*=}"
    value="${value%$'\r'}"
    value="${value%\"}"; value="${value#\"}"
    value="${value%\'}"; value="${value#\'}"
    printf '%s' "$value"
}

if [[ -z "${REDIS_URL:-}" ]]; then
    REDIS_URL="$(read_env_var REDIS_URL || true)"
fi
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
export REDIS_URL

# Разбираем хост, порт и пароль из REDIS_URL.
redis_hostport="${REDIS_URL#redis://}"
redis_hostport="${redis_hostport%%/*}"
redis_userinfo=""
if [[ "$redis_hostport" == *@* ]]; then
    redis_userinfo="${redis_hostport%@*}"
    redis_hostport="${redis_hostport##*@}"
fi
redis_host="${redis_hostport%%:*}"
redis_port="${redis_hostport##*:}"
if [[ "$redis_port" == "$redis_hostport" || -z "$redis_port" ]]; then
    redis_port="6379"
fi
redis_host="${redis_host:-127.0.0.1}"
redis_password=""
# Пароль — только часть после двоеточия в userinfo (`user:password` / `:password`).
if [[ "$redis_userinfo" == *:* ]]; then
    redis_password="${redis_userinfo#*:}"
fi

# Строка для вывода: пароль (если есть) маскируем, чтобы не утекал в консоль/логи.
redis_url_masked="$REDIS_URL"
if [[ -n "$redis_userinfo" ]]; then
    redis_url_masked="${REDIS_URL/\/\/${redis_userinfo}@/\/\/***@}"
fi

redis_ping() {
    command -v redis-cli >/dev/null 2>&1 || return 1
    redis-cli -u "$REDIS_URL" ping >/dev/null 2>&1
}

# Доступен ли Redis: пробуем `redis-cli`, а при его отсутствии — считаем
# доступным, если наш контейнер уже запущен (`docker inspect`). Так скрипт не
# пересоздаёт работающий контейнер на машинах без `redis-cli`.
redis_available() {
    if command -v redis-cli >/dev/null 2>&1; then
        redis_ping && return 0
        return 1
    fi
    command -v docker >/dev/null 2>&1 || return 1
    docker inspect -f '{{.State.Running}}' "$REDIS_CONTAINER" 2>/dev/null | grep -q true
}

echo "== 1. Проверяю Redis ($redis_url_masked) =="
if redis_available; then
    echo "Redis доступен."
else
    echo "Redis недоступен — подниму контейнер '$REDIS_CONTAINER'."
    if ! command -v docker >/dev/null 2>&1; then
        echo "! docker не найден. Запустите Redis вручную по $redis_url_masked." >&2
        exit 1
    fi
    if [[ "$redis_host" != "127.0.0.1" && "$redis_host" != "localhost" ]]; then
        echo "! REDIS_URL указывает на удалённый хост ($redis_host); локальный контейнер не поднимаю." >&2
        exit 1
    fi

    # Повторный запуск: имя контейнера может быть занято — снимаем его.
    # При первом запуске контейнера нет, ошибка подавляется.
    docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true

    run_args=(-d --name "$REDIS_CONTAINER" -p "${redis_port}:6379" redis:7-alpine)
    if [[ -n "$redis_password" ]]; then
        run_args+=(redis-server --requirepass "$redis_password")
    fi
    if ! docker run "${run_args[@]}" >/dev/null; then
        echo "! не удалось запустить контейнер '$REDIS_CONTAINER' (порт ${redis_port} занят?)." >&2
        exit 1
    fi

    echo -n "Жду готовности Redis"
    for _ in $(seq 1 50); do
        if redis_available; then
            echo " — готов."
            break
        fi
        echo -n "."
        sleep 0.1
    done
    if ! redis_available; then
        echo
        echo "! Redis не отвечает по $redis_url_masked после запуска контейнера." >&2
        exit 1
    fi
fi

echo "== 2. Собираю release-бинарь loadgen =="
cargo build --release --bin loadgen

echo "== 3. Запускаю нагрузочный прогон =="
if [[ $# -gt 0 ]]; then
    exec ./target/release/loadgen "$@"
else
    exec ./target/release/loadgen
fi
