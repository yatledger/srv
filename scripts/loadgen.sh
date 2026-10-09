#!/usr/bin/env bash
#
# Обёртка нагрузочного генератора dagdb.
#
# Собирает release-бинарь `loadgen`, проверяет доступность Redis и запускает
# нагрузочный сценарий на in-process кластере. Переданные аргументы заменяют
# набор по умолчанию (а не дополняют его), чтобы не плодить конфликтующие флаги.
#
# Переменные окружения:
#   REDIS_URL     строка подключения к Redis (по умолчанию redis://127.0.0.1:6379/0)
#
# Примеры:
#   ./scripts/loadgen.sh
#   ./scripts/loadgen.sh --tx 2000 --tps 1000 --parents 3
#
set -euo pipefail

cd "$(dirname "$0")/.."

REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"

echo "== 1. Проверяю Redis ($REDIS_URL) =="
if command -v redis-cli >/dev/null 2>&1; then
    if ! redis-cli -u "$REDIS_URL" ping >/dev/null 2>&1; then
        echo "! Redis недоступен по $REDIS_URL. Запустите: docker run -d -p 6379:6379 redis:7-alpine" >&2
        exit 1
    fi
    echo "Redis доступен."
else
    echo "! redis-cli не найден; проверка Redis пропущена (полагаюсь на fail-fast узлов)." >&2
fi

echo "== 2. Собираю release-бинарь loadgen =="
cargo build --release --bin loadgen

echo "== 3. Запускаю нагрузочный прогон =="
if [[ $# -gt 0 ]]; then
    exec ./target/release/loadgen "$@"
else
    exec ./target/release/loadgen
fi
