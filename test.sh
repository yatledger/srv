#!/usr/bin/env bash
#
# Локальный сценарий запуска кластера dagdb из 4 узлов + Redis.
#
# Требования:
#   - собранный бинарник (запустите `cargo build` или `./build.sh`);
#   - запущенный Redis (например: `docker run -d -p 6379:6379 redis:7-alpine`).
#
# Скрипт поднимает 4 узла в фоне, инициализирует кластер, грузит генезис
# и делает несколько запросов. По завершении (или по Ctrl-C) узлы гасятся.
#
# Все изменяемые параметры вынесены в переменные окружения ниже.

set -euo pipefail

BIN="${BIN:-./target/debug/dagdb}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
# Внутренний токен обязателен для /mng/*, /add, /load-genesis и /raft/*.
TOKEN="${INTERNAL_API_TOKEN:-local-dev-token}"
BASE_HOST="${BASE_HOST:-127.0.0.1}"
BASE_PORT="${BASE_PORT:-21001}"
DATA_ROOT="${DATA_DIR:-./data}"
LOG_DIR="${LOG_DIR:-./logs}"

PIDS=()
CLEANED=0

cleanup() {
    if [[ "$CLEANED" == "1" ]]; then
        return
    fi
    CLEANED=1
    echo "== Останавливаю узлы =="
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

start_node() {
    local id="$1" port="$2"
    local host="$BASE_HOST"
    local addr="$host:$port"
    echo "== Запускаю узел $id ($addr) =="
    NODE_ID="$id" \
    ADVERTISE_ADDR="$addr" \
    BIND_ADDR="0.0.0.0:$port" \
    HTTP_PORT="$port" \
    DATA_DIR="$DATA_ROOT/node$id" \
    REDIS_URL="$REDIS_URL" \
    INTERNAL_API_TOKEN="$TOKEN" \
    RUST_LOG="${RUST_LOG:-info}" \
    "$BIN" >"$LOG_DIR/node$id.log" 2>&1 &
    PIDS+=("$!")
}

api() {
    # api <port> <method> <path> [json-body]
    local port="$1" method="$2" path="$3" body="${4:-}"
    if [[ -n "$body" ]]; then
        curl -sS -X "$method" \
            -H "Content-Type: application/json" \
            -H "x-internal-token: $TOKEN" \
            -d "$body" \
            "http://$BASE_HOST:$port$path"
    else
        curl -sS -X "$method" \
            -H "x-internal-token: $TOKEN" \
            "http://$BASE_HOST:$port$path"
    fi
    echo
}

mkdir -p "$LOG_DIR" "$DATA_ROOT"

start_node 1 "$BASE_PORT"
start_node 2 "$((BASE_PORT + 1))"
start_node 3 "$((BASE_PORT + 2))"
start_node 4 "$((BASE_PORT + 3))"

# Ждём, пока HTTP-сервер первого узла начнёт отвечать (до ~15 секунд).
echo "== Ожидаю запуск узла 1 =="
ready=0
for _ in $(seq 1 30); do
    if curl -sS -o /dev/null "http://$BASE_HOST:$BASE_PORT/pool" 2>/dev/null; then
        ready=1
        break
    fi
    sleep 0.5
done
if [[ "$ready" != "1" ]]; then
    echo "! Узел 1 не поднялся; смотрите $LOG_DIR/node1.log"
    exit 1
fi

echo "== Инициализирую кластер из 4 узлов =="
api "$BASE_PORT" POST /mng/init \
    "[[1, \"$BASE_HOST:$BASE_PORT\"],[2, \"$BASE_HOST:$((BASE_PORT + 1))\"],[3, \"$BASE_HOST:$((BASE_PORT + 2))\"],[4, \"$BASE_HOST:$((BASE_PORT + 3))\"]]"

# Ждём выборов лидера (до ~15 секунд).
echo "== Ожидаю выборы лидера =="
leader=""
for _ in $(seq 1 30); do
    metrics="$(curl -sS -X POST -H "x-internal-token: $TOKEN" \
        "http://$BASE_HOST:$BASE_PORT/mng/metrics" 2>/dev/null || true)"
    if echo "$metrics" | grep -q '"current_leader":[0-9]'; then
        leader="$metrics"
        break
    fi
    sleep 0.5
done
if [[ -z "$leader" ]]; then
    echo "! Лидер не выбран за отведённое время; продолжаю (genesis может не загрузиться)."
fi

echo "== Загружаю genesis.json =="
api "$BASE_PORT" POST /load-genesis '[]'

echo "== Метрики узла 1 =="
api "$BASE_PORT" POST /mng/metrics

echo "== Публичный API: /pool =="
curl -sS "http://$BASE_HOST:$BASE_PORT/pool"; echo

echo "== Публичный API: /full =="
curl -sS "http://$BASE_HOST:$BASE_PORT/full"; echo

echo
echo "Узлы работают в фоне. Логи: $LOG_DIR/node{1..4}.log"
echo "Проверка без токена должна вернуть 401:"
curl -sS -o /dev/null -w "  /mng/metrics без токена -> %{http_code}\n" \
    -X POST "http://$BASE_HOST:$BASE_PORT/mng/metrics" || true

echo
echo "Нажмите Ctrl-C для остановки кластера."
wait
