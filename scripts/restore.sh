#!/usr/bin/env bash
#
# Восстановление узла dagdb из бэкапа (O5).
#
# Узел должен быть остановлен. Скрипт восстанавливает каталог данных и (если
# найден) дамп Redis, созданные scripts/backup.sh.
#
# Переменные окружения:
#   BACKUP_SNAPSHOT   путь к каталогу бэкапа (обязателен), напр. ./backups/20260101T000000Z
#   DATA_DIR          каталог данных узла (по умолчанию ./data)
#   REDIS_URL         строка подключения к Redis (для восстановления дампа)
#
# Пример:
#   BACKUP_SNAPSHOT=./backups/20260101T000000Z DATA_DIR=./data/node1 ./scripts/restore.sh

set -euo pipefail

: "${BACKUP_SNAPSHOT:?BACKUP_SNAPSHOT обязателен (путь к каталогу бэкапа)}"
DATA_DIR="${DATA_DIR:-./data}"

if [[ ! -d "$BACKUP_SNAPSHOT" ]]; then
    echo "! Каталог бэкапа не найден: $BACKUP_SNAPSHOT" >&2
    exit 1
fi

echo "== 1. Восстанавливаю DATA_DIR ($DATA_DIR) =="
if [[ -d "$BACKUP_SNAPSHOT/data" ]]; then
    rm -rf "$DATA_DIR"
    mkdir -p "$(dirname "$DATA_DIR")"
    cp -a "$BACKUP_SNAPSHOT/data" "$DATA_DIR"
else
    echo "! В бэкапе нет подкаталога data/" >&2
    exit 1
fi

echo "== 2. Восстанавливаю дамп Redis (если есть) =="
if [[ -f "$BACKUP_SNAPSHOT/dump.rdb" ]]; then
    if [[ -n "${REDIS_URL:-}" ]] && command -v redis-cli >/dev/null 2>&1; then
        # Останавливаем Redis, подменяем дамп и запускаем снова.
        redis-cli -u "$REDIS_URL" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
        redis_dir="$(redis-cli -u "$REDIS_URL" CONFIG GET dir 2>/dev/null | tail -1 || true)"
        if [[ -n "${redis_dir:-}" && -d "$redis_dir" ]]; then
            cp -a "$BACKUP_SNAPSHOT/dump.rdb" "$redis_dir/dump.rdb"
            echo "Дамп восстановлен в $redis_dir/dump.rdb (запустите Redis заново)"
        else
            echo "! Не удалось определить каталог Redis; скопируйте $BACKUP_SNAPSHOT/dump.rdb вручную" >&2
        fi
    else
        echo "! REDIS_URL/redis-cli недоступны; скопируйте $BACKUP_SNAPSHOT/dump.rdb вручную" >&2
    fi
else
    echo "Дамп Redis в бэкапе отсутствует — пропускаю"
fi

echo "== Готово. Запустите узел с DATA_DIR=$DATA_DIR =="
