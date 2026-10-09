#!/usr/bin/env bash
#
# Бэкап узла dagdb (O5).
#
# Порядок:
#   1. Просит узел построить свежий Raft-снапшот (POST /mng/snapshot).
#   2. Копирует каталог персистентных данных (redb: raft-log + state-machine).
#   3. Просит Redis выполнить BGSAVE и сохраняет дамп архива.
#
# Переменные окружения:
#   NODE_URL             базовый URL узла (по умолчанию http://127.0.0.1:21001)
#   INTERNAL_API_TOKEN   кластерный токен (обязателен)
#   DATA_DIR             каталог данных узла (по умолчанию ./data)
#   REDIS_URL            строка подключения к Redis (обязательна)
#   BACKUP_DIR           каталог для бэкапов (по умолчанию ./backups)
#
# Пример:
#   INTERNAL_API_TOKEN=... REDIS_URL=redis://127.0.0.1:6379/0 \
#     DATA_DIR=./data/node1 ./scripts/backup.sh

set -euo pipefail

NODE_URL="${NODE_URL:-http://127.0.0.1:21001}"
DATA_DIR="${DATA_DIR:-./data}"
BACKUP_DIR="${BACKUP_DIR:-./backups}"
: "${INTERNAL_API_TOKEN:?INTERNAL_API_TOKEN обязателен}"
: "${REDIS_URL:?REDIS_URL обязателен}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TARGET="$BACKUP_DIR/$STAMP"
mkdir -p "$TARGET"

echo "== 1. Форсирую снапшот Raft =="
curl -fsS -X POST -H "x-internal-token: $INTERNAL_API_TOKEN" \
    "$NODE_URL/mng/snapshot" >/dev/null

echo "== 2. Копирую DATA_DIR ($DATA_DIR) =="
if [[ ! -d "$DATA_DIR" ]]; then
    echo "! Каталог данных не найден: $DATA_DIR" >&2
    exit 1
fi
cp -a "$DATA_DIR" "$TARGET/data"

echo "== 3. Бэкапю дамп Redis =="
# Извлекаем хост/порт из REDIS_URL для redis-cli, если он доступен; иначе
# сохраняем через контейнер.
if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -u "$REDIS_URL" BGSAVE >/dev/null || true
    # Ждём завершения фонового сохранения.
    for _ in $(seq 1 30); do
        status="$(redis-cli -u "$REDIS_URL" INFO persistence 2>/dev/null \
            | tr -d '\r' | awk -F: '/rdb_bgsave_in_progress/{print $2}')"
        [[ "$status" == "0" ]] && break
        sleep 1
    done
    dump_path="$(redis-cli -u "$REDIS_URL" CONFIG GET dir 2>/dev/null | tail -1)/dump.rdb"
    if [[ -f "$dump_path" ]]; then
        cp -a "$dump_path" "$TARGET/dump.rdb"
    else
        echo "! Не удалось определить путь dump.rdb; пропускаю" >&2
    fi
else
    echo "! redis-cli не найден; сохраните дамп Redis вручную в $TARGET/dump.rdb" >&2
fi

echo "== Готово: $TARGET =="
echo "Содержимое:"
ls -la "$TARGET"
